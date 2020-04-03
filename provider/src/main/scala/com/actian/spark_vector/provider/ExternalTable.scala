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

object ExternalTable {

  def externalTableDataFrame(
    spark: SparkSession,
    extRef: String,
    format: String,
    schemaSpec: Option[String],
    options: Map[String, String],
    filters: Seq[Column]
  ): DataFrame = {
    val schema = schemaSpec.flatMap(SchemaParser.parseSchema)
    val reader = schema.foldLeft(spark.read.options(options))(_.schema(_))
    val df = format match {
      case "parquet" => reader.parquet(extRef)
      case "csv" => reader.csv(extRef)
      case "orc" => reader.orc(extRef)
      case _ => reader.format(format).load(extRef)
    }
    val filtered = filters.reduceLeftOption(_ and _).foldLeft(df)(_.filter(_))
    // enforce schema, workaround for https://issues.apache.org/jira/browse/SPARK-10848
    schema.fold(filtered)(spark.createDataFrame(filtered.rdd, _))
  }

}
