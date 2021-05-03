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

import fs2._

import java.io.ByteArrayInputStream
import java.util.UUID
import net.snowflake.client.jdbc.SnowflakeConnection
import org.slf4s.Logger

sealed trait StageFile {
  def fragment: Fragment
}

object StageFile {
  private val Compressed = true

  def apply(input: Chunk[Byte], connection: SnowflakeConnection, blocker: Blocker, logger: Logger)
      : Resource[ConnectionIO, StageFile] = {
    println(s"::::::::::::::::::::::::::::::::::::\n\n${new String(input.toBytes.values)}\n::::::::::::::::::::::::::::\n\n")
    val inputStream = new ByteArrayInputStream(input.toBytes.values)
    val debug = (s: String) => Sync[ConnectionIO].delay {
      logger.debug(s)
    }

    val acquire: ConnectionIO[StageFile] = for {
      unique <- Sync[ConnectionIO].delay(UUID.randomUUID.toString)
      name = s"precog-$unique"
      _ <- debug(s"Starting staging to file: @~/$name")
      _ <- blocker.delay[ConnectionIO, Unit](connection.uploadStream("@~", "/", inputStream, name, Compressed))
      _ <- debug(s"Finished staging to file: @~/$name")
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
}
