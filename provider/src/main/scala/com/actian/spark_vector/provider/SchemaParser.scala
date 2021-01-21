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
package com.actian.spark_vector.provider

import com.actian.spark_vector.util._
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.types._

import scala.language.postfixOps

object SchemaParser extends Logging {

  def parseSchema(schemaString: String): Option[StructType] = {
    def mkField(name: String, `type`: String, nullable: Boolean): StructField =
      StructField(name, CatalystSqlParser.parseDataType(`type`), nullable)
    def parseField(line: String): StructField =
      line.trim.split("\\s+").toList match {
        case List(n, t) => mkField(n, t, nullable = true)
        case List(n, t, rest@_*) =>
          rest.mkString(" ").toLowerCase match {
            case "not null" => mkField(n, t, nullable = false)
            case unknown => throw new IllegalArgumentException(s"Unknown field modifier: '$unknown'.")
          }
        case _ => throw new IllegalArgumentException(s"Illegal field spec: '$line'.")
      }
    schemaString.trim match {
      case "" => None
      case schemaString =>
        try {
          val schema = StructType(schemaString.split(',').map(parseField))
          logDebug(s"Parsed custom schema for external source: ${schema.simpleString}")
          Some(schema)
        }
        catch {
          case exc: Exception =>
            logWarning(s"Unable to parse schema for external source: $schemaString. Attempting read with default options.", exc)
            None
        }
    }
  }

}
