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
package com.actian.spark_vector.vector

import org.apache.spark.sql.types._

object TableSchemaGenerator {
  // TODO ensure consistency with ColumnMetadata mappings
  private def sqlTypeName(dataType: DataType): String = dataType match {
    case BooleanType => "boolean"
    case ByteType => "integer1"
    case ShortType => "smallint"
    case IntegerType => "integer"
    case LongType => "bigint"
    case FloatType => "float4"
    case DoubleType => "float"
    case dec: DecimalType => s"decimal(${dec.precision}, ${dec.scale})"
    case DateType => "ansidate"
    case TimestampType => "timestamp with time zone"
    case _ => "varchar(4096)"
  }

  private def columnSpec(field: StructField): String =
    s"${field.name} ${sqlTypeName(field.dataType)}${if (field.nullable) "" else " NOT NULL"}"

  /**
   * Given SparkSQL datatypes specified by `schema`, generate the VectorSQL create table
   * statement that matches the datatypes
   */
  def generateTableSQL(name: String, schema: StructType): String = {
    val columnSpecs = schema.fields.map(columnSpec)
    s"""|CREATE TABLE ${name} (
        |${columnSpecs.mkString("\t", ",\n\t", "")}
        |)
     """.stripMargin
  }
}
