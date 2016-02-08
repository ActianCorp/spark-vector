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

import org.apache.spark.sql.types._

/**
 * Wrap column metadata returned by JDBC and provide functions to support converting into a StructField.
 */
case class ColumnMetadata(val name: String, val typeName: String, val nullable: Boolean, val precision: Int, val scale: Int) extends Serializable {

  /**
   * Convert the given column metadata into a `StructField` representing the column
   * @return a new `StructField` instance
   */
  val structField: StructField = StructField(name, dataType, nullable)

  // Convert from column type name to StructField data type
  private[this] def dataType: DataType = {
    typeName match {
      case "boolean" => BooleanType
      case "integer1" => ByteType
      case "smallint" => ShortType
      case "integer" => IntegerType
      case "bigint" => LongType
      case "float4" => FloatType
      case "float" => DoubleType
      case "decimal" => DecimalType(precision, scale)
      case "money" => DecimalType(precision, scale)
      case "char" => StringType
      case "nchar" => StringType
      case "varchar" => StringType
      case "nvarchar" => StringType
      case "ansidate" => DateType
      case "time without time zone" => TimestampType
      case "time with time zone" => TimestampType
      case "time with local time zone" => TimestampType
      case "timestamp without time zone" => TimestampType
      case "timestamp with time zone" => TimestampType
      case "timestamp with local time zone" => TimestampType
      case "interval year to month" => StringType
      case "interval day to year" => StringType
      case _ => StringType
    }
  }

  override def toString: String = {
    "name: " + name + "; typeName: " + typeName + "; nullable: " + nullable + "; precision: " + precision + "; scale: " + scale
  }
}
