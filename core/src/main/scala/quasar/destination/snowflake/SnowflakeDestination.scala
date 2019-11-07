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
import scala.{Byte, StringContext, Unit}

import quasar.api.destination.{Destination, DestinationType, ResultSink}
import quasar.api.push.RenderConfig
import quasar.api.resource.ResourcePath
import quasar.api.table.{ColumnType, TableColumn}
import quasar.connector.{MonadResourceErr, ResourceError}

import cats.effect._
import cats.data._
import cats.implicits._

import doobie._
import doobie.implicits._

import fs2._

import java.io.InputStream
import java.lang.Exception

import net.snowflake.client.jdbc.SnowflakeConnection

import pathy.Path, Path.FileName

import scalaz.NonEmptyList

import shims._

final class SnowflakeDestination[F[_]: ConcurrentEffect: MonadResourceErr: Timer](xa: Transactor[F])
    extends Destination[F] {
  def destinationType: DestinationType =
    SnowflakeDestinationModule.destinationType

  def sinks: NonEmptyList[ResultSink[F]] =
    NonEmptyList(csvSink)

  private val csvSink: ResultSink[F] = ResultSink.csv[F](RenderConfig.Csv()) {
    case (path, columns, bytes) =>
      for {
        inputStream <- (io.toInputStream[F]: Pipe[F, Byte, InputStream])(bytes)
        fileName <- Stream.eval(ensureSingleSegment(path))

        connection <- Stream.resource(snowflakeConnection(xa))

        _ <- Stream.eval(
          Sync[F].delay(connection.uploadStream("@~", "/", inputStream, fileName.value, true)))

        cols0 <- columns.toNel.fold[Stream[F, NonEmptyList[TableColumn]]](
          Stream.raiseError[F](new Exception("No columns specified")))(
          _.asScalaz.pure[Stream[F, ?]])

        cols <- cols0.traverse(mkColumn(_)).fold(
          errs => Stream.raiseError[F](
            new Exception(s"Some column types are not supported: ${mkErrorString(errs.asScalaz)}")),
          c => Stream(c).covaryAll[F, NonEmptyList[Fragment]])

        tableQuery = createTableQuery(fileName.dropExtension.value, cols).query[Unit]

        _ = println(s"Cols: \n $cols \n")
        _ = println(s"Query: \n ${tableQuery.sql} \n")

        _ <- tableQuery.stream.transact(xa)

      } yield ()
  }

  private def mkErrorString(errs: NonEmptyList[ColumnType.Scalar]): String =
    errs.map(_.show).intercalate(", ")

  // we need to retrieve a SnowflakeConnection from the Hikari transactor.
  // this is the recommended way to access Snowflake-specific methods
  // https://docs.snowflake.net/manuals/user-guide/jdbc-using.html#unwrapping-snowflake-specific-classes
  private def snowflakeConnection(xa: Transactor[F]): Resource[F, SnowflakeConnection] =
    xa.connect(xa.kernel).map(_.unwrap(classOf[SnowflakeConnection]))

  private def ensureSingleSegment(r: ResourcePath): F[FileName] =
    r.fold(
      file =>
        if (Path.depth(file) === 1)
          Path.fileName(file).pure[F]
        else
          MonadResourceErr[F].raiseError(ResourceError.notAResource(r)),
      MonadResourceErr[F].raiseError(ResourceError.notAResource(r)))

  private def createTableQuery(tableName: String, columns: NonEmptyList[Fragment]): Fragment =
    (fr"CREATE TABLE " ++ Fragment.const(tableName)) ++ Fragments.parentheses(
      columns.intercalate(fr", "))

  private def mkColumn(c: TableColumn): ValidatedNel[ColumnType.Scalar, Fragment] =
    columnTypeToSnowflake(c.tpe).map(Fragment.const(c.name) ++ _)

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
      case ColumnType.Number => fr0"NUMBER(33, 3)".validNel
      case ColumnType.String => fr0"STRING".validNel
    }
}
