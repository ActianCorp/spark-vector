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
  // Get the sql type name based on the associated vector data type
  private def sqlTypeName(dataType: DataType): String = {
    val sqlType = VectorDataType(dataType).vectorSQLtype
    dataType match {
      // Add precision and scale if decimal
      case dec: DecimalType => sqlType + s"(${dec.precision}, ${dec.scale})"
      // Add default length of 4096 for varchar
      case _ => if( sqlType == "varchar" ) sqlType + "(4096)" else sqlType
    }
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
