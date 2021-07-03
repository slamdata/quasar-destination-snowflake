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

import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._

import doobie._
import doobie.implicits._

import fs2.{Pipe, Stream}
import fs2.concurrent.Queue
import fs2.io

import java.util.UUID
import net.snowflake.client.jdbc.SnowflakeConnection
import org.slf4s.Logger

sealed trait StageFile {
  def name: String
  def fragment = fr0"@~/" ++ Fragment.const0(name)
}

object StageFile {
  private val Compressed = true

  def apply[F[_]: ConcurrentEffect: ContextShift](
      input: Stream[F, Byte],
      connection: SnowflakeConnection,
      blocker: Blocker,
      xa: Transactor[F],
      logger: Logger)
      : Resource[F, StageFile] = {
    io.toInputStreamResource(input) flatMap { inputStream =>
      val debug = (s: String) => Sync[F].delay(logger.debug(s))

      val acquire: F[StageFile] = for {
        unique <- Sync[F].delay(UUID.randomUUID.toString)
        uniqueName = s"precog-$unique"
        _ <- debug(s"Starting staging to file: @~/$uniqueName")
        _ <- blocker.delay[F, Unit](connection.uploadStream("@~", "/", inputStream, uniqueName, Compressed))
        _ <- debug(s"Finished staging to file: @~/$uniqueName")
      } yield new StageFile {
        def name = uniqueName
      }

      val release: StageFile => F[Unit] = sf => {
        val fragment = fr"rm" ++ sf.fragment
        debug(s"Cleaning staging file @~/${sf.name} up")
        fragment.query[Unit].option.void.transact(xa)
      }

      Resource.make(acquire)(release)
    }
  }

  def eventPipe[F[_]: ConcurrentEffect: ContextShift, A](
      connection: SnowflakeConnection,
      offsets: Queue[F, Option[A]],
      blocker: Blocker,
      xa: Transactor[F],
      logger: Logger)
      : Pipe[F, Event[F, A], Stream[F, StageFile]] = {
    def handle(
        ev: Event[F, A],
        accum: Ref[F, List[Resource[F, StageFile]]])
        : Stream[F, Option[List[Resource[F, StageFile]]]] =
      ev match {
        case Event.Commit(value) =>
          for {
            current <- Stream.eval(accum.getAndSet(List[Resource[F, StageFile]]()))
            _ <- Stream.eval(offsets.enqueue1(value.some))
          } yield current.some
        case e: Event.Create[F] =>
          for {
            _ <- Stream.eval(accum.update { (lst: List[Resource[F, StageFile]]) =>
              lst :+ StageFile(e.value, connection, blocker, xa, logger)
            })
          } yield none[List[Resource[F, StageFile]]]
      }
    inp => Stream.eval(Ref.of[F, List[Resource[F, StageFile]]](List[Resource[F, StageFile]]())) flatMap { acc =>
      inp
        .onFinalize(offsets.enqueue1(none[A]))
        .flatMap(handle(_, acc))
        .unNone
        .map { (lst: List[Resource[F, StageFile]]) =>
          Stream.emits(lst.map(Stream.resource)).flatten
        }
    }
  }
}
