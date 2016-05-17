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
import org.apache.spark.sql.DataFrame

class RequestHandler(sqlContext: SQLContext, val auth: ProviderAuth) extends Logging {
  import Job._

  private val id = new AtomicLong(0L)

  private def run(implicit socket: SocketChannel): Future[JobResult] = Future {
    val json = DataStreamReader.readWithByteBuffer() { DataStreamReader.readString _ }
    logDebug(s"Got new json request: ${json}")
    val job: Job = Json.fromJson[Job](Json.parse(json)).fold(errors => {
      throw new IllegalArgumentException(s"Invalid JSON receive: $json.\nThe errors are: ${JsError.toFlatJson(errors)}")
    }, job => job)

    for {
      part <- job.parts
      format <- part.format.orElse {
        throw new IllegalArgumentException(s"All part jobs must have the format specified, but query ${job.query_id}, part ${part.part_id} doesn't")
      }
    } {
      val vectorDf = vectorDF(part)
      part.operator_type match {
        case "scan" => {
          val vectorTable = register(part.external_table_name, vectorDf)
          val select = selectStatement(format, part)
          sqlContext.sql(s"insert into ${sparkQuote(vectorTable)} $select")
        }
        case "insert" => writeDF(format, vectorDf, part)
        case _ => throw new IllegalArgumentException(s"Unknown operator type: ${part.operator_type} in job ${job.query_id}, part ${part.part_id}")
      }
    }
    JobResult(job.transaction_id, job.query_id, success = Some(true))
  }

  def handle(implicit socket: SocketChannel): Unit = run onComplete {
    case Success(result) => handleSuccess(result)
    case Failure(t) => handleFailure(t)
  }

  private def handleSuccess(result: JobResult)(implicit socket: SocketChannel) = {
    logInfo(s"Job tr_id:${result.transaction_id}, query_id:${result.query_id} has succeeded")
    socket.close
  }

  private def handleFailure(cause: Throwable)(implicit socket: SocketChannel) = {
    logError(s"Job has failed with exception ${cause}: ${cause.getMessage}\n${cause.getStackTraceString}")
    socket.close
  }

  private def vectorDF(part: JobPart): DataFrame = {
    val rel = VectorRelation(part.column_infos.map(_.toColumnMetadata), part.writeConf, sqlContext)
    sqlContext.baseRelationToDataFrame(rel)
  }

  private def register(prefix: String, df: DataFrame): String = {
    val ret = s"${prefix}_${id.incrementAndGet}"
    df.registerTempTable(ret)
    ret
  }

  private def selectStatement(format: String, part: JobPart): String = {
    val options = part.options.getOrElse(Map.empty[String, String])
    val df = format match {
      case "parquet" => sqlContext.read.options(options).parquet(part.external_reference)
      case "csv" => sqlContext.read.options(options).format("com.databricks.spark.csv").load(part.external_reference)
      case "orc" => sqlContext.read.options(options).orc(part.external_reference)
      case _ => sqlContext.read.options(options).format(format).load(part.external_reference)
    }
    val inputTable = s"in_${part.external_table_name}_${id.incrementAndGet}"
    df.registerTempTable(inputTable)
    s"select ${part.column_infos.map(ci => sparkQuote(ci.column_name)).mkString(", ")} from ${sparkQuote(inputTable)}"
  }

  private def writeDF(format: String, df: DataFrame, part: JobPart): Unit = {
    val options = part.options.getOrElse(Map.empty[String, String])
    format match {
      case "parquet" => df.write.options(options).parquet(part.external_reference)
      case "csv" => df.write.options(options).format("com.databricks.spark.csv").save(part.external_reference)
      case "orc" => df.write.options(options).orc(part.external_reference)
      case _ => df.write.options(options).format(format).save(part.external_reference)
    }
  }
}
