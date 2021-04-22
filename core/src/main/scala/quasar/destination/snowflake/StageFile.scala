package quasar.destination.snowflake

import slamdata.Predef._

import cats.effect._
import cats.implicits._

import doobie.Fragment

import fs2.Stream
import fs2.io

import java.util.UUID
import net.snowflake.client.jdbc.SnowflakeConnection

sealed trait StageFile {
  def fragment: Fragment
}

object StageFile {
  val Compressed = true
  def apply[F[_]: Sync](input: Stream[F, Byte], blocker: Blocker): Resource[F, StageFile] =
    io.toInputStreamResource(input) flatMap { is =>
      val acquire: F[(String, StageFile)] = for {
        prefix <- Sync[F].delay(UUID.randomUUID.toString)
        name = s"precog_$name"
        _ <- blocker.delay[F, Unit](connection.uploadStream("@~", "/", inputStream, name, Compressed))
      } yield new StageFile {
        def fragment = Fragment.const(name)
      }
      val release: StageFile => F[Unit]

      }
}
