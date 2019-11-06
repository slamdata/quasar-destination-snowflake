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
import scala.Byte

import quasar.api.destination.{Destination, DestinationType, ResultSink}
import quasar.api.push.RenderConfig
import quasar.api.resource.ResourcePath
import quasar.connector.{MonadResourceErr, ResourceError}

import cats.effect._
import cats.implicits._

import doobie.Transactor

import fs2._

import java.io.InputStream

import net.snowflake.client.jdbc.SnowflakeConnection

import pathy.Path, Path.FileName

import scalaz.NonEmptyList

final class SnowflakeDestination[F[_]: ConcurrentEffect: MonadResourceErr: Timer](
  xa: Transactor[F]) extends Destination[F] {
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
        _ <- Stream.eval(Sync[F].delay(connection.uploadStream("@~", "/", inputStream, fileName.value, true)))
      } yield ()
  }

  // we need to retrieve a SnowflakeConnection from the Hikari transactor.
  // this is the recommended way to access Snowflake-specific methods
  // https://docs.snowflake.net/manuals/user-guide/jdbc-using.html#unwrapping-snowflake-specific-classes
  private def snowflakeConnection(xa: Transactor[F]): Resource[F, SnowflakeConnection] =
    xa.connect(xa.kernel).map(_.unwrap(classOf[SnowflakeConnection]))

  private def ensureSingleSegment(r: ResourcePath): F[FileName] =
    r.fold(file =>
      if (Path.depth(file) === 1)
        Path.fileName(file).pure[F]
      else
        MonadResourceErr[F].raiseError(ResourceError.notAResource(r)),
      MonadResourceErr[F].raiseError(ResourceError.notAResource(r)))
}
