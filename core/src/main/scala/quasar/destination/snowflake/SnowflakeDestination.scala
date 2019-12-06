/*
 * Copyright 2014–2019 SlamData Inc.
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

import scala.Predef._
import scala.{Boolean, Byte, StringContext, Unit}

import quasar.api.destination.{LegacyDestination, DestinationColumn, DestinationType, ResultSink}
import quasar.api.push.RenderConfig
import quasar.api.resource._
import quasar.api.table.ColumnType
import quasar.connector.{MonadResourceErr, ResourceError}

import cats.effect._
import cats.data._
import cats.implicits._

import doobie._
import doobie.implicits._

import fs2._

import org.slf4s.Logging

import java.lang.Exception
import java.util.UUID

import net.snowflake.client.jdbc.SnowflakeConnection

import pathy.Path, Path.FileName

final class SnowflakeDestination[F[_]: ConcurrentEffect: MonadResourceErr: Timer](xa: Transactor[F])
    extends LegacyDestination[F] with Logging {

  def destinationType: DestinationType =
    SnowflakeDestinationModule.destinationType

  def sinks: NonEmptyList[ResultSink[F, Type]] =
    NonEmptyList.one(csvSink)

  private val Compressed: Boolean = true

  private val csvSink: ResultSink[F, Type] = ResultSink.csv[F, Type](RenderConfig.Csv()) {
    case (path, columns, bytes) =>
      Stream.force(
        for {
          freshNameSuffix <- Sync[F].delay(UUID.randomUUID().toString)
          freshName = s"reform-$freshNameSuffix"
          push = doPush(path, columns, bytes, freshName).onFinalize(removeFile(freshName))
        } yield push)
  }

  private def removeFile(fileName: String): F[Unit] =
    (fr0"rm @~/" ++ Fragment.const(fileName)) // no risk of injection here since this is fresh name
      .query[Unit].stream.transact(xa).compile.drain

  private def doPush(
      path: ResourcePath,
      columns: NonEmptyList[DestinationColumn[Type]],
      bytes: Stream[F, Byte],
      freshName: String)
      : Stream[F, Unit] =
    for {
      fileName <- Stream.eval(ensureSingleSegment(path))

      inputStream <- bytes.through(io.toInputStream)
      connection <- Stream.resource(snowflakeConnection(xa))

      _ <- debug(s"Starting staging to file: @~/$freshName")

      _ <- Stream.eval(
        Sync[F].delay(connection.uploadStream("@~", "/", inputStream, freshName, Compressed)))

      _ <- debug(s"Finished staging to file: @~/$freshName")

      cols <- columns.traverse(mkColumn(_)).fold(
        errs => Stream.raiseError[F](
          new Exception(s"Some column types are not supported: ${mkErrorString(errs)}")),
        c => Stream(c).covaryAll[F, NonEmptyList[Fragment]])

      tableQuery = createTableQuery(fileName.value, cols).query[String]
      loadQuery = loadTableQuery(freshName, fileName.value, Compressed).query[String]

      _ <- debug(s"Table creation query:\n${tableQuery.sql}")
      _ <- debug(s"Load query:\n${loadQuery.sql}")

      createTableResponse <- tableQuery.stream.transact(xa)
      loadTableResponse <- loadQuery.stream.transact(xa)

      _ <- debug(s"Create table response: $createTableResponse")
      _ <- debug(s"Load table response: $loadTableResponse")

    } yield ()

  private def loadTableQuery(stagedFile: String, tableName: String, compressed: Boolean): Fragment = {
    val fileToLoad =
      if (compressed)
        stagedFile + ".gz"
      else
        stagedFile

    fr"COPY INTO" ++
      Fragment.const(escapeString(tableName)) ++
      fr0" FROM @~/" ++
      Fragment.const(fileToLoad) ++ // no risk of injection here since this is fresh name
      fr"""file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"')"""
  }

  // Double quote the string and remove any stray double quotes inside
  // the string, they can't be part of valid identifiers in Snowflake
  // anyway.
  private def escapeString(str: String): String =
    s""""${str.replace("\"", "")}""""

  private def mkErrorString(errs: NonEmptyList[ColumnType.Scalar]): String =
    errs.map(_.show).intercalate(", ")

  // we need to retrieve a SnowflakeConnection from the Hikari transactor.
  // this is the recommended way to access Snowflake-specific methods
  // https://docs.snowflake.net/manuals/user-guide/jdbc-using.html#unwrapping-snowflake-specific-classes
  private def snowflakeConnection(xa: Transactor[F]): Resource[F, SnowflakeConnection] =
    xa.connect(xa.kernel).map(_.unwrap(classOf[SnowflakeConnection]))

  private def ensureSingleSegment(r: ResourcePath): F[FileName] =
    r match {
      case file /: ResourcePath.Root => FileName(file).pure[F]
      case _ => MonadResourceErr[F].raiseError(ResourceError.notAResource(r))
    }

  private def createTableQuery(tableName: String, columns: NonEmptyList[Fragment]): Fragment =
    (fr"CREATE OR REPLACE TABLE" ++ Fragment.const(escapeString(tableName))) ++ Fragments.parentheses(
      columns.intercalate(fr", "))

  private def mkColumn(c: DestinationColumn[Type]): ValidatedNel[ColumnType.Scalar, Fragment] =
    columnTypeToSnowflake(c.tpe).map(Fragment.const(escapeString(c.name)) ++ _)

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

  private def debug(msg: String): Stream[F, Unit] =
    Stream.eval(Sync[F].delay(log.debug(msg)))
}
