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
import cats.effect.concurrent.{Ref, Semaphore}
import cats.implicits._

import doobie._
import doobie.implicits._

import fs2._
import fs2.concurrent.Queue
import fs2.io

import java.io.ByteArrayInputStream
import java.util.UUID
import net.snowflake.client.jdbc.SnowflakeConnection
import org.slf4s.Logger
import java.nio.charset.StandardCharsets

sealed trait StageFile[F[_]] {
  def ingest(chunk: Chunk[Byte]): F[Unit]
  def done: Resource[F, Fragment]
}

object StageFile {
  private val Compressed = true

  private final case class StageFileState[F[_]](
      q: Queue[F, Option[Chunk[Byte]]],
      name: String)

  def apply[F[_]: ConcurrentEffect: ContextShift](
      xa: Transactor[F],
      connection: SnowflakeConnection,
      blocker: Blocker,
      logger: Logger)
      : F[StageFile[F]] = {
    val debug = (s: String) => Sync[F].delay(logger.debug(s))

    for {
      rq <- Ref.of[F, Option[StageFileState[F]]](None)
      semaphore <- Semaphore[F](1)
    } yield {
      def getOrStart: F[StageFileState[F]] = rq.get flatMap {
        case Some(q) => q.pure[F]
        case None => for {
          q <- Queue.unbounded[F, Option[Chunk[Byte]]]
          unique <- Sync[F].delay(UUID.randomUUID.toString)
          name = s"precog_$unique"
          state = StageFileState(q, name)
          _ <- semaphore.withPermit {
            rq.set(state.some) >>
            ConcurrentEffect[F].start {
              debug(s"Starting staging to file: @~/$name") >>
              io.toInputStreamResource(q.dequeue.unNoneTerminate.flatMap(Stream.chunk(_))).use({ is =>
                blocker.delay[F, Unit](connection.uploadStream("@~", "/", is, name, Compressed))
              }) >>
              debug(s"Finished staging to file: @~/$name")
            }
          }
        } yield state
      }
      new StageFile[F] {
        def ingest(c: Chunk[Byte]): F[Unit] =
          getOrStart.flatMap(_.q.enqueue1(c.some))
        def done: Resource[F, Fragment] = Resource.liftF(getOrStart) flatMap { state =>
          val acquire =
            state.q.enqueue1(None) >>
            semaphore.withPermit(rq.set(None)) as
            Fragment.const0(state.name)
          val release: Fragment => F[Unit] = sf => {
            val fragment = fr0"rm @~/" ++ sf
            debug("Cleaning staging file @~/$name up") >>
            fragment.query[Unit].option.void.transact(xa)
          }
          Resource.make(acquire)(release)
        }
      }
    }
  }
/*
  def apply(input: Chunk[Byte], connection: SnowflakeConnection, blocker: Blocker, logger: Logger)
      : Resource[ConnectionIO, StageFile] = {
    val bytes = input.toBytes.values
    val inputStream = new ByteArrayInputStream(bytes)
    val msg =
      s"::::::::::::::::::::::::::::::::::::\n\n${new String(bytes, StandardCharsets.UTF_8)}\n::::::::::::::::::::::::::::\n\n"
    val debug = (s: String) => Sync[ConnectionIO].delay {
      logger.debug(s)
    }

    val acquire: ConnectionIO[StageFile] = for {
      _ <- debug(msg)
      unique <- Sync[ConnectionIO].delay(UUID.randomUUID.toString)
      name = s"precog-$unique"
      _ <- blocker.delay[ConnectionIO, Unit](connection.uploadStream("@~", "/", inputStream, name, Compressed))
    } yield new StageFile {
      def fragment = Fragment.const0(name)
    }

    val release: StageFile => ConnectionIO[Unit] = sf => {
      val fragment = fr0"rm @~/" ++ sf.fragment
      debug("Cleaning staging file @~/$name up") >>
      fragment.query[Unit].option.void
    }

    Resource.make(acquire)(release)
  }
  */
}
