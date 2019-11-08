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
import scala.StringContext

import argonaut._, Argonaut._

import cats.implicits._

final case class AccountName(value: String)
final case class User(value: String)
final case class Password(value: String)
final case class DatabaseName(value: String)
final case class Schema(value: String)
final case class Warehouse(value: String)

final case class SnowflakeConfig(
  accountName: AccountName,
  user: User,
  password: Password,
  databaseName: DatabaseName,
  schema: Schema,
  warehouse: Warehouse)

object SnowflakeConfig {
  implicit val snowflakeConfigCodecJson: CodecJson[SnowflakeConfig] =
    casecodec6[String, String, String, String, String, String, SnowflakeConfig](
      (an, usr, pass, dbName, schema, wh) =>
        SnowflakeConfig(
          AccountName(an),
          User(usr),
          Password(pass),
          DatabaseName(dbName),
          Schema(schema),
          Warehouse(wh)),
      cfg =>
        (cfg.accountName.value,
          cfg.user.value,
          cfg.password.value,
          cfg.databaseName.value,
          cfg.schema.value,
          cfg.warehouse.value).some
    )("accountName", "user", "password", "databaseName", "schema", "warehouse")

  def configToUri(config: SnowflakeConfig): String =
    s"jdbc:snowflake://${config.accountName.value}.snowflakecomputing.com/?db=${config.databaseName.value}&schema=${config.schema.value}&warehouse=${config.warehouse.value}"
}
