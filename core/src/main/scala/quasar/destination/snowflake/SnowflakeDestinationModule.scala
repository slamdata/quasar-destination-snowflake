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
import scala._

import quasar.api.destination.DestinationError.InitializationError
import quasar.api.destination.{Destination, DestinationError, DestinationType}
import quasar.connector.{DestinationModule, MonadResourceErr}
import quasar.concurrent.NamedDaemonThreadFactory

import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext
import scala.util.{Either, Random}

import argonaut._, Argonaut._

import cats.data.EitherT
import cats.effect._

import doobie.hikari.HikariTransactor

import eu.timepit.refined.auto._

object SnowflakeDestinationModule extends DestinationModule {
  def destinationType: DestinationType =
    DestinationType("snowflake", 1L)

  val SnowflakeDriverFqcn = "net.snowflake.client.jdbc.SnowflakeDriver"
  val Redacted = "<REDACTED>"
  val PoolSize: Int = 10

  def sanitizeDestinationConfig(config: Json): Json =
    config.as[SnowflakeConfig].result.fold(_ => Json.jEmptyObject, cfg =>
      cfg.copy(user = User(Redacted), password = Password(Redacted)).asJson)

  def destination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
    config: Json): Resource[F, Either[InitializationError[Json], Destination[F]]] =
    (for {
      config <- EitherT.fromEither[Resource[F, ?]](config.as[SnowflakeConfig].result) leftMap {
        case (err, _) => DestinationError.malformedConfiguration((destinationType, config, err))
      }
      poolSuffix <- EitherT.right(Resource.liftF(Sync[F].delay(Random.alphanumeric.take(5).mkString)))
      connectPool <- EitherT.right(boundedPool(s"snowflake-dest-connect-$poolSuffix", PoolSize))
      transactPool <- EitherT.right(unboundedPool(s"snowflake-dest-transact-$poolSuffix"))
      jdbcUri = SnowflakeConfig.configToUri(config)
      transactor <- EitherT.right[InitializationError[Json]](
        HikariTransactor.newHikariTransactor(
          SnowflakeDriverFqcn,
          jdbcUri,
          config.user.value,
          config.password.value,
          connectPool,
          transactPool))
      destination: Destination[F] = new SnowflakeDestination[F](transactor)
    } yield destination).value

  private def boundedPool[F[_]: Sync](name: String, threadCount: Int): Resource[F, ExecutionContext] =
    Resource.make(
      Sync[F].delay(
        Executors.newFixedThreadPool(
          threadCount,
          NamedDaemonThreadFactory(name))))(es => Sync[F].delay(es.shutdown()))
      .map(ExecutionContext.fromExecutor(_))

  private def unboundedPool[F[_]: Sync](name: String): Resource[F, ExecutionContext] =
    Resource.make(
      Sync[F].delay(
        Executors.newCachedThreadPool(
          NamedDaemonThreadFactory(name))))(es => Sync[F].delay(es.shutdown()))
      .map(ExecutionContext.fromExecutor(_))

}
