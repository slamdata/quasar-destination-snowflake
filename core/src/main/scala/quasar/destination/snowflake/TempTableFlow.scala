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

import cats.data.{NonEmptyList, ValidatedNel}
import cats.effect._
import cats.effect.concurrent.Ref
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
  def apply[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr](
      xa: Transactor[F],
      logger: Logger,
      writeMode: WriteMode,
      schema: String,
      hygienicIdent: String => String,
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

    val acquire: F[(TempTable, Flow[Byte], StageFile[F])] = for {
      tbl <- args.path match {
        case file /: ResourcePath.Root => file.pure[F]
        case _ => MonadResourceErr[F].raiseError(ResourceError.notAResource(args.path))
      }
      columnFragments <- args.columns.traverse(mkColumn(hygienicIdent, _)).fold(
        errs => Sync[F].raiseError {
          ColumnTypesNotSupported(errs)
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
        hygienicIdent,
        args.filterColumn)
      _ <- {
        tempTable.drop >>
        tempTable.create >>
        commit
      }.transact(xa)
      refMode <- Ref.in[F, ConnectionIO, QWriteMode](args.writeMode)
      conn <- unwrap(classOf[SnowflakeConnection]).transact(xa)
      stageFile <- StageFile(xa, conn, blocker, logger)
    } yield {
      val toConnectionIO = Effect.toIOK[F] andThen LiftIO.liftK[ConnectionIO]

      val flow = new Flow[Byte] {
        def delete(ids: IdBatch): ConnectionIO[Unit] =
          ().pure[ConnectionIO]

        def ingest(chunk: Chunk[Byte]): ConnectionIO[Unit] =
          toConnectionIO(stageFile.ingest(chunk))

        def replace: ConnectionIO[Unit] =
          refMode.get flatMap {
            case QWriteMode.Replace =>
              stageFile.done.mapK(toConnectionIO).use(_.traverse_(tempTable.ingest)) >> commit >>
              tempTable.persist >> commit >> refMode.set(QWriteMode.Append)
            case QWriteMode.Append =>
              append
          }

        def append: ConnectionIO[Unit] =
          stageFile.done.mapK(toConnectionIO).use(_.traverse(tempTable.ingest)) >> commit >>
          tempTable.append >> commit
      }
      (tempTable, flow, stageFile)
    }

    val release: ((TempTable, _, StageFile[F])) => F[Unit] = { case (tempTable, _, stageFile) =>
      stageFile.done.use(_ => ().pure[F]) >>
      (tempTable.drop >> commit).transact(xa)
    }

    Resource.make(acquire)(release).map(_._2)
  }

  private trait TempTable {
    def ingest(stageFile: Fragment): ConnectionIO[Unit]
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
        hygienicIdent: String => String,
        filterColumn: Option[Column[_]])
        : TempTable = {

      val tmpName = s"precog_tmp_$tableName"
      val tmpFragment = Fragment.const0(hygienicIdent(schema)) ++ fr0"." ++ Fragment.const0(hygienicIdent(tmpName))
      val tgtFragment = Fragment.const0(hygienicIdent(schema)) ++ fr0"." ++ Fragment.const0(hygienicIdent(tableName))

      def runFragment: Fragment => ConnectionIO[Unit] = { fr =>
        fr.updateWithLogHandler(log).run.void
      }

      def createTgt = runFragment {
        fr"CREATE TABLE" ++ tgtFragment ++
          Fragments.parentheses(columnFragments.intercalate(fr","))
      }

      def createTgtIfNotExists = runFragment {
        fr"CREATE TABLE IF NOT EXISTS" ++ tgtFragment ++
          Fragments.parentheses(columnFragments.intercalate(fr","))
      }

      def dropTgtIfExists = runFragment {
        fr"DROP TABLE IF EXISTS" ++ tgtFragment
      }

      def truncateTgt = runFragment {
        fr"TRUNCATE" ++ tgtFragment
      }

      def truncate = runFragment {
        fr"TRUNCATE" ++ tmpFragment
      }

      def rename = runFragment {
        fr"ALTER TABLE" ++ tmpFragment ++ fr" RENAME TO" ++ tgtFragment
      }

      new TempTable {
        def ingest(fragment: Fragment): ConnectionIO[Unit] = runFragment {
          fr"COPY INTO" ++ tmpFragment ++ fr0" FROM @~/" ++
          fragment ++
          fr""" file_format = (type = csv, skip_header = 0, field_optionally_enclosed_by = '"', escape = none, escape_unenclosed_field = none)"""
        }

        def drop: ConnectionIO[Unit] = runFragment {
          fr"DROP TABLE IF EXISTS" ++ tmpFragment
        }

        def create: ConnectionIO[Unit] = runFragment {
          fr"CREATE TABLE IF NOT EXISTS" ++ tmpFragment ++
            Fragments.parentheses(columnFragments.intercalate(fr","))
        }

        // This might be something like filterColumn.fold(insertInto)(merge)
        def merge: ConnectionIO[Unit] =
          filterColumn.traverse_(filter(_)) >>
          insertInto

        def filter(column: Column[_]): ConnectionIO[Unit] = runFragment {
          val mkColumn: String => Fragment = parent =>
            Fragment.const0(parent) ++ fr0"." ++
            Fragment.const0(hygienicIdent(column.name))

          fr"DELETE FROM" ++ tgtFragment ++ fr" target" ++
          fr"USING" ++ tmpFragment ++ fr" temp" ++
          fr"WHERE" ++ mkColumn("target") ++ fr0"=" ++ mkColumn("temp")
        }

        def insertInto: ConnectionIO[Unit] = runFragment {
          val colFragments = columns.map { (col: Column[_]) =>
            Fragment.const0(hygienicIdent(col.name))
          }
          val allColumns = colFragments.intercalate(fr",")
          val toInsert = Fragments.parentheses(allColumns)

          fr"INSERT INTO" ++ tgtFragment ++ fr0" " ++ toInsert ++ fr0" " ++
          fr"SELECT" ++ allColumns ++ fr" FROM" ++ tmpFragment
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

  private def mkColumn(hygienicIdent: String => String, c: Column[ColumnType.Scalar])
      : ValidatedNel[ColumnType.Scalar, Fragment] =
    columnTypeToSnowflake(c.tpe)
      .map(Fragment.const(hygienicIdent(c.name)) ++ _)

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
