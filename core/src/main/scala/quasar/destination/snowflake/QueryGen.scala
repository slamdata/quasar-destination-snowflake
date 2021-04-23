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

import scala.Predef._
import scala._
import scala.util.matching.Regex

object QueryGen {
  // we should eventually move the query generation code here to make it more testable
  def sanitizeIdentifier(str: String, cfg: Boolean): String =
    if (cfg)
      // replace all non-alphanumeric characters with _ and make all characters uppercase
      (new Regex("""(\W)""")).replaceAllIn(str, "_").toUpperCase
    else
      // Double quote the string and remove any stray double quotes inside
      // the string, they can't be part of valid identifiers in Snowflake
      // anyway.
      s""""${str.replace("\"", "")}""""
}
