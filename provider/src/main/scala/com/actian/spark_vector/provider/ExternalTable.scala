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

import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.internal.Logging

object ExternalTable extends Logging {

  def externalTableDataFrame(
      spark: SparkSession,
      extRef: String,
      format: String,
      schemaSpec: Option[String],
      options: Map[String, String],
      filters: Seq[Column],
      columnInfos: Option[Seq[ColumnInfo]]
  ): DataFrame = {

    def createSchema(x: Seq[ColumnInfo]): StructType = {
      val schema = StructType(x.map(_.toColumnMetadata).map(_.structField))
      logDebug(s"Using schema deduced from column infos:${schema.simpleString}")
      schema
    }

    val schema = schemaSpec.fold(
      columnInfos.fold[Option[StructType]](None)(x => Some(createSchema(x)))
    )(SchemaParser.parseSchema(_))
    val readerWithOptions = spark.read.options(options)
    val readerWithOptionsAndSchema = schema.fold(
      readerWithOptions.option("inferSchema", "true")
    )(readerWithOptions.schema(_))
    val df = format match {
      case "parquet" => readerWithOptionsAndSchema.parquet(extRef)
      case "csv"     => readerWithOptionsAndSchema.csv(extRef)
      case "orc"     => readerWithOptionsAndSchema.orc(extRef)
      case _         => readerWithOptionsAndSchema.format(format).load(extRef)
    }

    val filtered = filters.reduceLeftOption(_ and _).foldLeft(df)(_.filter(_))
    // enforce schema, workaround for https://issues.apache.org/jira/browse/SPARK-10848
    schema.fold(filtered)(spark.createDataFrame(filtered.rdd, _))
  }
}
