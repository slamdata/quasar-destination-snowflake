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
import cats.implicits._

import doobie._
import doobie.implicits._

import fs2.Stream
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
    val debug = (s: String) => Sync[F].delay(logger.debug(s))

    val acquire: F[StageFile] = for {
      unique <- Sync[F].delay(UUID.randomUUID.toString)
      uniqueName = s"precog-$unique"
    } yield new StageFile {
      def name = uniqueName
    }

    def ingest(sf: StageFile): F[StageFile] =
      io.toInputStreamResource(input) use { inputStream =>
        for {
          _ <- debug(s"Starting staging to file: @~/${sf.name}")
          _ <- blocker.delay[F, Unit](connection.uploadStream("@~", "/", inputStream, sf.name, Compressed))
          _ <- debug(s"Finished staging to file: @~/${sf.name}")
        } yield sf
      }

    val release: StageFile => F[Unit] = sf => {
      val fragment = fr"rm" ++ sf.fragment
      debug(s"Cleaning staging file @~/${sf.name} up") >>
      fragment.query[Unit].option.void.transact(xa)
    }

    Resource.make(acquire)(release).evalMap(ingest(_))
  }
}
