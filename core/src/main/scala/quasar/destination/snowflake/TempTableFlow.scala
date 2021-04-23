/*
 * Copyright 2020 Precog Data
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.destination.snowflake

import slamdata.Predef._
import scala.Predef.classOf

import quasar.api.{Column, ColumnType}
import quasar.api.resource._
import quasar.concurrent.NamedDaemonThreadFactory
import quasar.connector.{MonadResourceErr, ResourceError, IdBatch}
import quasar.connector.destination.{WriteMode => QWriteMode}
import quasar.lib.jdbc.Slf4sLogHandler
import quasar.lib.jdbc.destination.WriteMode
import quasar.lib.jdbc.destination.flow.{Flow, FlowArgs}

import cats.Alternative
import cats.data.{Validated, NonEmptyList, ValidatedNel}
import cats.effect._
import cats.effect.concurrent.Ref
import cats.effect.syntax.bracket._
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.free.connection.{commit, unwrap}

import fs2.Chunk

import org.slf4s.Logger

import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors

import net.snowflake.client.jdbc.SnowflakeConnection

object TempTableFlow {
  def apply[F[_]: Sync: MonadResourceErr](
      xa: Transactor[F],
      logger: Logger,
      writeMode: WriteMode,
      schema: String,
      identCfg: Boolean,
      args: FlowArgs[ColumnType.Scalar])
      : Resource[F, Flow[Byte]] = {

    val log = Slf4sLogHandler(logger)

    def checkWriteMode(tableName: String): F[Unit] = {
      val fragment =
        fr0"SELECT count(*) as exists_flag from information_schema.tables where table_schema = '" ++
        Fragment.const0(tableName) ++ fr0"' AND table_schema = '" ++
        Fragment.const0(schema) ++ fr0"'"

      val existing =
        fragment.queryWithLogHandler[Int](log).option.map(_.exists(_ === 1))

      writeMode match {
        case WriteMode.Create => existing.transact(xa) flatMap { exists =>
          MonadResourceErr[F].raiseError(
            ResourceError.accessDenied(
              args.path,
              "Create mode is set but table exists already".some,
              none)).whenA(exists)
        }
        case _ =>
          ().pure[F]
      }
    }

    val acquire: F[(TempTable, Flow[Byte])] = for {
      tbl <- args.path match {
        case file /: ResourcePath.Root => file.pure[F]
        case _ => MonadResourceErr[F].raiseError(ResourceError.notAResource(args.path))
      }
      columnFragments <- args.columns.traverse(mkColumn(identCfg, _)).fold(
        errs => Sync[F].raiseError {
          new Exception(s"Some column types are not supported: ${mkErrorString(errs)}")
        },
        _.pure[F])
      _ <- checkWriteMode(tbl)
      totalBytes <- Ref.in[F, ConnectionIO, Long](0L)
      tempTable = TempTable(
        log,
        totalBytes,
        writeMode,
        tbl,
        schema,
        args.columns,
        columnFragments,
        identCfg,
        args.filterColumn)
      _ <- {
        tempTable.drop >>
        tempTable.create >>
        commit
      }.transact(xa)
      refMode <- Ref.in[F, ConnectionIO, QWriteMode](args.writeMode)
    } yield {
      val flow = new Flow[Byte] {
        def delete(ids: IdBatch): ConnectionIO[Unit] =
          ().pure[ConnectionIO]

        def connection: ConnectionIO[SnowflakeConnection] =
          unwrap(classOf[SnowflakeConnection])

        def ingest(chunk: Chunk[Byte]): ConnectionIO[Unit] = for {
          conn <- connection
          file <- StageFile(chunk, conn, blocker).use(tempTable.ingest)
          _ <- commit
        } yield ()

        def replace: ConnectionIO[Unit] = refMode.get flatMap {
          case QWriteMode.Replace =>
            tempTable.persist >> commit >> refMode.set(QWriteMode.Append)
          case QWriteMode.Append =>
            append
        }

        def append: ConnectionIO[Unit] =
          tempTable.append >> commit
      }
      (tempTable, flow)
    }

    val release: ((TempTable, _)) => F[Unit] = { case (tempTable, _) =>
      (tempTable.drop >> commit).transact(xa)
    }

    Resource.make(acquire)(release).map(_._2)
  }

  private trait TempTable {
    def ingest(stageFile: StageFile): ConnectionIO[Unit]
    def drop: ConnectionIO[Unit]
    def create: ConnectionIO[Unit]
    def persist: ConnectionIO[Unit]
    def append: ConnectionIO[Unit]
  }


  private object TempTable {
    def apply(
        log: LogHandler,
        totalBytes: Ref[ConnectionIO, Long],
        writeMode: WriteMode,
        tableName: String,
        schema: String,
        columns: NonEmptyList[Column[ColumnType.Scalar]],
        columnFragments: NonEmptyList[Fragment],
        identCfg: Boolean,
        filterColumn: Option[Column[_]])
        : TempTable = {
      val tmpFragment = fr""
      val tgtFragment = fr""

      def createTgt = {
        val fragment = fr"CREATE TABLE" ++ tgtFragment ++
          Fragments.parentheses(columnFragments.intercalate(fr","))
        fragment.updateWithLogHandler(log).run.void
      }

      def createTgtIfNotExists = {
        val fragment = fr"CREATE TABLE IF NOT EXISTS" ++ tgtFragment ++
          Fragments.parentheses(columnFragments.intercalate(fr","))
        fragment.updateWithLogHandler(log).run.void
      }

      def dropTgtIfExists = {
        val fragment = fr"DROP TABLE IF EXISTS" ++ tgtFragment
        fragment.updateWithLogHandler(log).run.void
      }

      def truncateTgt = {
        val fragment = fr"TRUNCATE" ++ tgtFragment
        fragment.updateWithLogHandler(log).run.void
      }


      def truncate = {
        val fragment = fr"TRUNCATE" ++ tmpFragment
        fragment.updateWithLogHandler(log).run.void
      }

      def rename = {
        val fragment = fr"ALTER TABLE" ++ tmpFragment ++ fr" RENAME TO" ++
          Fragment.const0(QueryGen.sanitizeIdentifier(tableName, identCfg))

        fragment.updateWithLogHandler(log).run.void
      }

      new TempTable {
        def ingest(stageFile: StageFile): ConnectionIO[Unit] =
          println("TODO").pure[ConnectionIO]

        def drop: ConnectionIO[Unit] = {
          val fragment = fr"DROP TABLE IF EXISTS" ++ tmpFragment
          fragment.updateWithLogHandler(log).run.void
        }

        def create: ConnectionIO[Unit] = {
          val fragment = fr"CREATE TABLE IF NOT EXISTS" ++ tmpFragment ++
            Fragments.parentheses(columnFragments.intercalate(fr","))
          fragment.updateWithLogHandler(log).run.void
        }

        // This might be something like filterColumn.fold(insertInto)(merge)
        def merge: ConnectionIO[Unit] =
          filterColumn.traverse_(filter(_)) >>
          insertInto

        def filter(column: Column[_]): ConnectionIO[Unit] = {
          val mkColumn: String => Fragment = parent =>
            Fragment.const0(parent) ++ fr0"." ++
            Fragment.const0(QueryGen.sanitizeIdentifier(column.name, identCfg))

          val fragment =
            fr"DELETE FROM" ++ tgtFragment ++ fr"target" ++
            fr"USING" ++ tmpFragment ++ fr"temp" ++
            fr"WHERE" ++ mkColumn("target") ++ fr0"=" ++ mkColumn("temp")

          fragment.updateWithLogHandler(log).run.void
        }

        def insertInto: ConnectionIO[Unit] = {
          val fragment = fr"INSERT INTO" ++ tgtFragment ++ fr0" " ++
            fr"SELECT * FROM" ++ tmpFragment

          fragment.updateWithLogHandler(log).run.void
        }

        def persist: ConnectionIO[Unit] = writeMode match {
          case WriteMode.Create =>
            createTgt >>
            append
          case WriteMode.Replace =>
            dropTgtIfExists >>
            rename >>
            create
          case WriteMode.Truncate =>
            createTgtIfNotExists >>
            truncateTgt >>
            append
          case WriteMode.Append =>
            createTgtIfNotExists >>
            append
        }

        def append: ConnectionIO[Unit] =
          merge >>
          truncate
      }
    }
  }

  private def blocker: Blocker =
    Blocker.liftExecutionContext(
      ExecutionContext.fromExecutor(
        Executors.newCachedThreadPool(NamedDaemonThreadFactory("snowflake-destination"))))

  private def mkColumn(identCfg: Boolean, c: Column[ColumnType.Scalar])
      : ValidatedNel[ColumnType.Scalar, Fragment] =
    columnTypeToSnowflake(c.tpe)
      .map(Fragment.const(QueryGen.sanitizeIdentifier(c.name, identCfg)) ++ _)

  private def mkErrorString(errs: NonEmptyList[ColumnType.Scalar]): String =
    errs.map(_.show).intercalate(", ")

  private def columnTypeToSnowflake(ct: ColumnType.Scalar)
      : ValidatedNel[ColumnType.Scalar, Fragment] =
    ct match {
      case ColumnType.Null => fr0"BYTEINT".validNel
      case ColumnType.Boolean => fr0"BOOLEAN".validNel
      case ColumnType.LocalTime => fr0"TIME".validNel
      case ot @ ColumnType.OffsetTime => ot.invalidNel
      case ColumnType.LocalDate => fr0"DATE".validNel
      case od @ ColumnType.OffsetDate => od.invalidNel
      case ColumnType.LocalDateTime => fr0"TIMESTAMP_NTZ".validNel
      case ColumnType.OffsetDateTime => fr0"TIMESTAMP_TZ".validNel
      case i @ ColumnType.Interval => i.invalidNel
      // this is an arbitrary precision and scale
      case ColumnType.Number => fr0"NUMBER(33, 3)".validNel
      case ColumnType.String => fr0"STRING".validNel
    }
}
