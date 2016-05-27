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
import org.apache.spark.sql.{ DataFrame, SQLContext }

import com.actian.spark_vector.datastream.reader.DataStreamReader
import com.actian.spark_vector.datastream.writer.DataStreamWriter
import com.actian.spark_vector.loader.command.sparkQuote
import com.actian.spark_vector.sql.VectorRelation
import com.actian.spark_vector.util.ResourceUtil.closeResourceAfterUse

import play.api.libs.json.{ JsError, Json }
import org.apache.spark.sql.SaveMode

/**
 * Handler for requests from Vector
 *
 * @param sqlContext context to use
 * @param auth authentication information to be verified on each request
 */
class RequestHandler(sqlContext: SQLContext, val auth: ProviderAuth) extends Logging {
  import Job._

  private val id = new AtomicLong(0L)
  private final val RequestPktType = 6

  /** Given a socket connection, read the JSON request for external resources and process its parts */
  private def run(implicit socket: SocketChannel): Future[JobResult] = Future {
    if (auth.doAuthentication)
      auth.srpServer.authenticate
    val json = DataStreamReader.readWithByteBuffer() { in =>
      if (in.getInt != RequestPktType)
        throw new IllegalArgumentException(s"Invalid packet type received for query request")
      DataStreamReader.readString(in)
    }
    logDebug(s"Got new json request: ${json}")
    Json.fromJson[Job](Json.parse(json)).fold(errors => {
      throw new IllegalArgumentException(s"Invalid JSON receive: $json.\nThe errors are: ${JsError.toFlatJson(errors)}")
    }, job => job)
  } flatMap { job =>
    /** An accumulator to ensure parts are run sequentially */
    var jobPartAccum = Future { () }
    for { part <- job.parts } {
      jobPartAccum = jobPartAccum flatMap (_ => Future[Unit] {
        val format = part.format.orElse(PartialFunction.condOpt[String, String](part.external_reference.split("\\.").last) {
          _ match {
            case "csv" => "csv"
            case "parquet" => "parquet"
            case "json" => "json"
            case "orc" => "orc"
            case "avro" => "avro"
          }
        }).getOrElse {
          throw new IllegalArgumentException(s"""Could not derive format of external reference ${part.external_reference},
            in part ${part.part_id}, trid=${job.transaction_id}, qid=${job.query_id}. Please specify an explicit format in the 'create external table' SQL definition.""")
        }
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
      }.transform(identity, JobException(_, job, part)))
    }
    jobPartAccum.map { _ => JobResult(job.transaction_id, job.query_id, success = Some(JobSuccess)) }
  }

  /** Start a future to handle a new external datasource query request */
  def handle(implicit socket: SocketChannel): Unit = run onComplete {
    case Success(result) => handleSuccess(result)
    case Failure(t) => handleFailure(t)
  }

  /** Write the job `result` to the socket, in JSON */
  private def writeJobResult(result: JobResult)(implicit socket: SocketChannel) =
    DataStreamWriter.writeWithByteBuffer { DataStreamWriter.writeNullTerminatedString(_, Json.toJson(result).toString) }

  /** Handle the success of a query request */
  private def handleSuccess(result: JobResult)(implicit socket: SocketChannel) = closeResourceAfterUse(socket) {
    logInfo(s"Job tr_id:${result.transaction_id}, query_id:${result.query_id} has succeeded")
    writeJobResult(result)
  }

  /** Handle the failure of a query request */
  private def handleFailure(cause: Throwable)(implicit socket: SocketChannel) = closeResourceAfterUse(socket) {
    val result = cause match {
      case JobException(e, job, part) => {
        logInfo(s"Job tr_id=${job.transaction_id}, query_id=${job.query_id} failed for part ${part.part_id}", cause)
        JobResult(job.transaction_id, job.query_id, error = Some(Seq(JobMsg(Some(part.part_id), msg = cause.getMessage, stacktrace = Some(cause.getStackTraceString)))))
      }
      case _ => {
        logError("Job failed while receiving/parsing json", cause)
        JobResult(-1, -1, error = Some(Seq(JobMsg(msg = cause.getMessage, stacktrace = Some(cause.getStackTraceString)))))
      }
    }
    writeJobResult(result)
  }

  /** Given a job part, create the dataframe to subsequently be used to read/insert data into Vector */
  private def vectorDF(part: JobPart): DataFrame = {
    val rel = VectorRelation(part.column_infos.map(_.toColumnMetadata), part.conf, sqlContext)
    sqlContext.baseRelationToDataFrame(rel)
  }

  /** Helper function to register a dataframe as a temp table with a given `prefix` */
  private def register(prefix: String, df: DataFrame): String = {
    val ret = s"${prefix}_${id.incrementAndGet}"
    df.registerTempTable(ret)
    ret
  }

  /**
   * Given a job part of "scan" type, generate the SparkSQL statement that can be used to retrieve data from Vector
   *
   * @param format the format of the external data source
   */
  private def selectStatement(format: String, part: JobPart): String = {
    val options = part.options.getOrElse(Map.empty[String, String])
    val table = format match {
      case "hive" => part.external_reference
      case _ => {
        val df = format match {
          case "parquet" => sqlContext.read.options(options).parquet(part.external_reference)
          case "csv" => sqlContext.read.options(options).format("com.databricks.spark.csv").load(part.external_reference)
          case "orc" => sqlContext.read.options(options).orc(part.external_reference)
          case _ => sqlContext.read.options(options).format(format).load(part.external_reference)
        }
        val inputTable = s"in_${part.external_table_name}_${id.incrementAndGet}"
        df.registerTempTable(inputTable)
        inputTable
      }
    }

    s"select ${part.column_infos.map(ci => sparkQuote(ci.column_name)).mkString(", ")} from ${sparkQuote(table)}"
  }

  /**
   * Given a job part of "insert" type, write the data contained in `df` to Vector
   *
   * @param format the format of the external data source
   */
  private def writeDF(format: String, df: DataFrame, part: JobPart): Unit = {
    val options = part.options.getOrElse(Map.empty[String, String])
    /** TODO(): Make this user configurable? */
    val mode = SaveMode.Append
    format match {
      case "parquet" => df.write.mode(mode).options(options).parquet(part.external_reference)
      case "csv" => df.write.mode(mode).options(options).format("com.databricks.spark.csv").save(part.external_reference)
      case "orc" => df.write.mode(mode).options(options).orc(part.external_reference)
      case "hive" => df.write.mode(mode).saveAsTable(part.external_reference)
      case _ => df.write.mode(mode).options(options).format(format).save(part.external_reference)
    }
  }
}
