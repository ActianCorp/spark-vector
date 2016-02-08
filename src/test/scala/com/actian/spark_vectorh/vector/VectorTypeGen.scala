/*
 * Copyright 2016 Actian Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.actian.spark_vectorh.vector

import org.scalacheck.Gen
import org.scalacheck.Gen.oneOf

object VectorTypeGen {
  // Generate JDBC types returned by Vector
  val vectorJdbcTypeGen: Gen[String] =
    oneOf(
      "boolean",
      "integer1",
      "smallint",
      "integer",
      "bigint",
      "float4",
      "float",
      "decimal",
      "money",
      "char",
      "nchar",
      "varchar",
      "nvarchar",
      "ansidate",
      "time without time zone",
      "time with time zone",
      "time with local time zone",
      "timestamp without time zone",
      "timestamp with time zone",
      "timestamp with local time zone",
      "interval year to month",
      "interval day to year")
}
