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

import slamdata.Predef._

import quasar.api.destination.{Destination, DestinationType, ResultSink}
import quasar.connector.MonadResourceErr

import cats.effect.{Effect, Timer}

import doobie.Transactor

import eu.timepit.refined.auto._

import scalaz.NonEmptyList

final class SnowflakeDestination[F[_]: Effect: MonadResourceErr: Timer](
  xa: Transactor[F]) extends Destination[F] {
  def destinationType: DestinationType =
    DestinationType("snowflake", 1L)

  def sinks: NonEmptyList[ResultSink[F]] =
    NonEmptyList(csvSink)

  private val csvSink: ResultSink.Csv[F] = ???
}
