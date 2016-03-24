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

import scala.util.parsing.json.JSON
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import com.actian.spark_vector.loader.command.VectorTempTable
import com.actian.spark_vector.writer.VectorEndPoint
import org.apache.spark.Logging
import com.actian.spark_vector.writer.WriteConf
import com.actian.spark_vector.loader.command._
import org.apache.spark.sql.DataFrame
import com.actian.spark_vector.sql.VectorRelation
import com.actian.spark_vector.sql.TableRef
import play.api.libs.json._
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonMappingException

object Main extends App with Logging {
  private final val QueryId = "query_id"
  private final val JobParts = "job_parts"
  private final val PartId = "part_id"
  private final val VectorTableName = "vector_table_name"
  private final val SqlQuery = "spark_sql_query"
  private final val ColsToLoad = "cols_to_load"
  private final val Ref = "spark_ref"
  private final val Options = "options"
  private final val Format = "format"
  private final val DataStreams = "datastreams"
  private final val DataStreamHost = "host"
  private final val DataStreamPort = "port"
  private final val UserName = "username"
  private final val Password = "password"

  private val conf = new SparkConf()
    .setAppName("Spark-Vector external tables provider")
    .set("spark.task.maxFailures", "1")
  private val sc = new SparkContext(conf)
  private val sqlContext = new HiveContext(sc)

  private lazy val vectorHost = "schwalbe"
  private lazy val vectorInstance = "C2"
  private lazy val vectorDatabase = "test"
  private lazy val connectionParams = Map("host" -> vectorHost, "instance" -> vectorInstance, "database" -> vectorDatabase)

  @inline def finished(line: String): Boolean = {
    line == null || line == "<END>"
  }

  logInfo("Spark-Vector provider initialized and starting listening for requests...")

  /** Parse numeric values as Longs */
  JSON.globalNumberParser = { input: String => input.toLong }
  Iterator.continually(Console.readLine).takeWhile(!finished(_)).foreach { line =>
    val jobOpt: Option[Job] = Json.fromJson[Job](Json.parse(line)).fold(errors => {
      logError(s"JSON received is invalid\n${JsError.toJson(errors)}")
      None
    }, job => Some(job))

    for {
      job <- jobOpt
      part <- job.job_parts
      format <- {
        part.options.get(Format).orElse {
          logError(s"All part jobs must have the format specified in their options, but query ${job.query_id}, part ${part.part_id} doesn't")
          None
        }
      }
    } try {
      logDebug("Got new JSON request...")
      val additionalOpts = Seq("table" -> part.vector_table_name) ++ (if (part.cols_to_load.isDefined) Seq("cols" -> part.cols_to_load.get.mkString(", ")) else Nil)
      val vectorRel = VectorRelation(TableRef(connectionParams ++ additionalOpts), None, sqlContext, Some(WriteConf(part.datastreams)))
      sqlContext.baseRelationToDataFrame(vectorRel).registerTempTable(part.vector_table_name)
      val inputTable = s"in_${part.vector_table_name}"
      val optionsWithoutFormat = part.options.filterKeys(_ != Format)
      val inputDf: DataFrame = format match {
        case "parquet" => sqlContext.read.options(optionsWithoutFormat).parquet(part.spark_ref)
        case "csv" => sqlContext.read.options(optionsWithoutFormat).format("com.databricks.spark.csv").load(part.spark_ref)
      }
      inputDf.registerTempTable(inputTable)
      val query = s"""insert into ${sparkQuote(part.vector_table_name)} ${part.spark_sql_query.replace(sparkQuote(s"${part.vector_table_name}"), sparkQuote(inputTable))}"""
      sqlContext.sql(query)
    } catch {
      case e: Exception =>
        logError(s"Job:${job.query_id},part:${part.part_id} failed with exception: ${e.getMessage}")
        logError(e.getStackTraceString)
    }
  }
}
