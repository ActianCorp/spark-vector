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

import java.nio.channels.SocketChannel
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{ Failure, Success }

import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext

import com.actian.spark_vector.datastream.reader.DataStreamReader
import com.actian.spark_vector.loader.command.sparkQuote
import com.actian.spark_vector.sql.VectorRelation

import play.api.libs.json.{ JsError, Json }

case class RequestResult(jobId: String, result: String)

class RequestHandler(sqlContext: SQLContext, val auth: ProviderAuth) extends Logging {
  import Job._

  private val id = new AtomicLong(0L)

  private def run(implicit socket: SocketChannel): Future[RequestResult] = Future {
    val json = DataStreamReader.readWithByteBuffer() { DataStreamReader.readString _ }
    logDebug(s"Got new json request: ${json}")
    val job: Job = Json.fromJson[Job](Json.parse(json)).fold(errors => {
      throw new IllegalArgumentException(s"Invalid JSON receive: $json.\nThe errors are: ${JsError.toFlatJson(errors)}")
    }, job => job)

    for {
      part <- job.parts
      format <- part.format.orElse {
        throw new IllegalArgumentException(s"All part jobs must have the format specified in their options, but query ${job.query_id}, part ${part.part_id} doesn't")
      }
    } {
      val vectorTable = register(part)
      val select = selectStatement(format, part)

      sqlContext.sql(s"insert into ${sparkQuote(vectorTable)} $select")
    }
    RequestResult(job.query_id.toString, "Success")
  }

  def handle(implicit socket: SocketChannel): Unit = run onComplete {
    case Success(result) => handleSuccess(result)
    case Failure(t) => handleFailure(t)
  }

  private def handleSuccess(result: RequestResult)(implicit socket: SocketChannel) = {
    logInfo(s"Job ${result.jobId} has succeeded")
    socket.close
  }

  private def handleFailure(cause: Throwable)(implicit socket: SocketChannel) = {
    logError(s"Job has failed with exception ${cause}: ${cause.getMessage}\n${cause.getStackTraceString}")
    socket.close
  }

  private def register(part: JobPart): String = {
    val rel = VectorRelation(part.external_table_name, part.column_infos.map(_.toColumnMetadata), part.writeConf, sqlContext)
    val ret = s"${part.external_table_name}_${id.incrementAndGet}"
    sqlContext.baseRelationToDataFrame(rel).registerTempTable(ret)
    ret
  }

  private def selectStatement(format: String, part: JobPart): String = {
    val optionsWithoutFormat = part.options.filterKeys(_ != Format)
    val df = format match {
      case "parquet" => sqlContext.read.options(optionsWithoutFormat).parquet(part.external_reference)
      case "csv" => sqlContext.read.options(optionsWithoutFormat).format("com.databricks.spark.csv").load(part.external_reference)
      case "orc" => sqlContext.read.options(optionsWithoutFormat).orc(part.external_reference)
      case _ => sqlContext.read.options(optionsWithoutFormat).format(format).load(part.external_reference)
    }
    val inputTable = s"in_${part.external_table_name}_${id.incrementAndGet}"
    df.registerTempTable(inputTable)
    s"select ${part.column_infos.map(ci => sparkQuote(ci.column_name)).mkString(", ")} from ${sparkQuote(inputTable)}"
  }
}
