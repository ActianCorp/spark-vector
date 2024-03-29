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

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.compat.Platform.EOL
import scala.util.{ Failure, Success }
import scala.util.matching.Regex

import org.apache.spark.sql.{ Column, DataFrame, Row, SaveMode, SparkSession }
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.StructType

import com.actian.spark_vector.datastream.reader.DataStreamReader
import com.actian.spark_vector.datastream.writer.DataStreamWriter
import com.actian.spark_vector.sql._
import com.actian.spark_vector.util.Logging
import com.actian.spark_vector.util.ResourceUtil.closeResourceAfterUse
import com.actian.spark_vector.vector.VectorNet

import play.api.libs.json.{ JsError, Json }
import resource.managed
import com.actian.spark_vector.vector.PredicatePushdown
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import resource.ManagedResource

/**
 * Handler for requests from Vector
 *
 * @param sqlContext context to use
 * @param auth authentication information to be verified on each request
 */
class RequestHandler(spark: SparkSession, val auth: ProviderAuth) extends Logging {
  import Job._

  private final val RequestPktType = 6 /* X100CPT_PROVIDER_REQUEST */
  private final val ProviderVersion = 1 /* ET_V_1 */
  private final val ExpectedClientType = 3 /* CLIENTTYPE_ETPROVIDER */

  /** Given a socket connection, read the JSON request for external resources and process its parts */
  private def run(implicit socket: SocketChannel): Future[JobResult] = Future {
    VectorNet.serverCheckVersion(ProviderVersion)
    if (auth.doAuthentication) auth.srpServer.authenticate
    VectorNet.readClientType(ExpectedClientType)
    val json = DataStreamReader.readWithByteBuffer() { in =>
      if (in.getInt != RequestPktType) throw new IllegalArgumentException(s"Invalid packet type received for query request")
      DataStreamReader.readString(in)
    }
    logDebug(s"Got new json request: ${json}")
    Json.fromJson[Job](Json.parse(json)).fold(errors => {
      throw new IllegalArgumentException(s"Invalid JSON receive: $json.\nThe errors are: ${JsError.toJson(errors)}")
    }, job => job)
  } flatMap { job =>
    logInfo(s"Received request for tr_id:${job.transaction_id}, query_id:${job.query_id}")
    /** An accumulator to ensure parts are run sequentially */
    var jobPartAccum: Future[Unit] = Future[Unit] {}
    for { part <- job.parts } {
      jobPartAccum = jobPartAccum flatMap (_ => Future[Unit] {
        SecurityChecks.checkUserPasswordProvided(part)
        part.operator_type match {
          case "scan" => handleScan(part)
          case "insert" => handleInsert(part)
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

  /** Given a throwable, get its message + of all its causes */
  private def getMessage(cause: Throwable): String = Iterator.iterate(cause)(_.getCause).takeWhile(_ != null).mkString(", caused by: ")

  /** Handle the failure of a query request */
  private def handleFailure(cause: Throwable)(implicit socket: SocketChannel) = closeResourceAfterUse(socket) {
    val result = cause match {
      case JobException(e, job, part) => {
        val message = getMessage(e)
        logInfo(s"Job tr_id:${job.transaction_id}, query_id:${job.query_id} failed for part ${part.part_id}. Reason: $message")
        logDebug(s"tr_id:${job.transaction_id}, query_id:${job.query_id}", cause)
        JobResult(job.transaction_id, job.query_id, error = Some(Seq(JobMsg(Some(part.part_id), msg = message, stacktrace = Some(cause.getStackTrace().mkString("", EOL, EOL))))))
      }
      case _ => {
        val message = getMessage(cause)
        logError("Job failed while receiving/parsing json", cause)
        JobResult(-1, -1, error = Some(Seq(JobMsg(msg = message, stacktrace = Some(cause.getStackTrace().mkString("", EOL, EOL))))))
      }
    }
    writeJobResult(result)
  }

  /** Given a job part, create the dataframe to subsequently be used to read/insert data into Vector */
  private def getVectorDF(part: JobPart): DataFrame = {
    val rel = VectorRelation(part.column_infos.map(_.toColumnMetadata), part.conf, spark.sqlContext)
    spark.baseRelationToDataFrame(rel)
  }

  /** Given a job part, retrieve its extra options, if any, or else an empty Map */
  private def getExtraOptions(part: JobPart): Map[String, String] =
    part.options.getOrElse(Map.empty[String, String])
    .map({case (k,v) => k.toLowerCase -> v})
    .filterKeys(k => part.extraOptions.contains(k.toLowerCase()))

  /**
   * Given a job part, return the format of the external table, according to the following logic:
   * 1) return the explicit part.format, if specified
   * 2) return the extension of part.external_reference, if recognized
   * 3) throw IllegalArgumentException otherwise
   */
  private def getFormat(part: JobPart): String = {
    val ret = part.format.orElse(PartialFunction.condOpt[String, String](part.external_reference.split("\\.").last) {
      ext =>
        ext match {
          case "csv" | "parquet" | "json" | "orc" | "avro" => ext
        }
    }).getOrElse {
      throw new IllegalArgumentException(s"""Could not derive format of external reference ${part.external_reference},
        in part ${part.part_id}. Please specify an explicit format in the 'create external table' SQL definition.""")
    }
    checkFormat(ret)
    ret
  }

  /** Ensure the format is supported by the environment */
  private def checkFormat(format: String): Unit = format match {
    case "hive" | "orc" if !spark.conf.get("spark.sql.catalogImplementation").equals("hive") =>
      throw new IllegalStateException(s"Reading ${format} sources requires Hive support. To enable this, set spark.vector.provider.hive to true in the spark_provider.conf file")
    case _ =>
  }

  /** Get a list of the filters that should be applied to the columns */
  private def getAllFilters(part: JobPart): Seq[Column] = {
    val extraOptions = getExtraOptions(part)
    val baseFilter = extraOptions.getOrElse("filter", "")
    if (!baseFilter.isEmpty())
      expr(baseFilter) +: PredicatePushdown.getFilters(part.column_infos.map(_.toColumnMetadata), spark.sparkContext)
    else
      PredicatePushdown.getFilters(part.column_infos.map(_.toColumnMetadata), spark.sparkContext)
  }

  /** Gets all filters as a spark sql string */
  private def getWhereString(part: JobPart): String = {
    val filters = getAllFilters(part)
    if (filters.nonEmpty)
      "where " + filters.reduceLeft(_ and _).expr.sql
    else
      ""
  }

  private def getStagingTableSQL(part: JobPart): (String, String) = {
    val extraOptions = getExtraOptions(part)
    val sql = extraOptions.getOrElse("staging", "")
    val Pattern = """(.*)THIS_TABLE(.*)""".r
    sql match {
      case Pattern(before, after) => (before, after)
      case _ => ("","")
    }
  }

  /** Given job part, return the corresponding SparkSqlTable that one may then "select * from" */
  private def getExternalTable(part: JobPart): SparkSqlTable = {
    val format = getFormat(part)

    format match {
      case "hive" => HiveTable(part.external_reference)
      case _ =>
         val options = Utils.getOptions(part)
         val schemaSpec = getExtraOptions(part).get("schema")
         val filters = getAllFilters(part)
         val df = ExternalTable.externalTableDataFrame(spark, part.external_reference, format, schemaSpec, options, filters, Some(part.column_infos))
         TempTable("src", df)
    }
  }

  private def getStagingTable(part: JobPart, sourceTable: String): Option[SparkSqlTable] = {
    val stagingSql = getStagingTableSQL(part)
    stagingSql match {
      case ("","") => None
      case(s1, s2) => {
        val df = spark.sql(s1 + sourceTable + s2)
        Some(TempTable("staging", df))
      }
      case _ => None
    }
  }

  /** Handle a job part of type "scan" by inserting into the VectorDF all tuples read from the external table. */
  private def handleScan(part: JobPart): Unit = {
    require(part.operator_type == "scan")
    for {
      externalTable <- managed(getExternalTable(part))
      stagingTable <- getStagingTable(part, externalTable.quotedName).fold[ManagedResource[SparkSqlTable]](managed(new SkeletonTable()))(x => managed(x))
      vectorTable <- managed(TempTable(part.external_table_name, getVectorDF(part)))
    } {
      val tableName = if (!stagingTable.isInstanceOf[SkeletonTable]) stagingTable.quotedName else externalTable.quotedName
      //Need to apply any filters for hive tables here
      val whereClause = if (getFormat(part).equals("hive")) getWhereString(part) else ""
      if (part.column_infos.isEmpty) {
        val selectCountStarStatement = s"select count(*) from ${tableName} ${whereClause}"
        val numTuples = spark.sql(selectCountStarStatement).first().getLong(0)
        val numEndpoints = part.datastream.streams_per_node.map(_.nr).sum
        val rddNoCols = spark.sparkContext.range(0L, numTuples, numSlices = numEndpoints).map(_ => Row.empty)
        val dfNoCols = spark.createDataFrame(rddNoCols, StructType(Seq.empty))
        dfNoCols.write.mode(SaveMode.Append).insertInto(vectorTable.tableName)
      } else {
        val cols = colsSelectStatement(Some(part.column_infos.map(_.column_name)))
        val selectStatement = s"select $cols from ${tableName} ${whereClause}"
        val wholeStatement = s"insert into table ${vectorTable.quotedName} $selectStatement"
        logDebug(s"SparkSql statement issued for reading external data into vector: $wholeStatement")
        spark.sql(wholeStatement)
      }
    }
  }

  /** Handle a job part of type "insert" by writing into the external table all tuples coming from the VectorDF. */
  private def handleInsert(part: JobPart): Unit = {
    require(part.operator_type == "insert")
    val df = getVectorDF(part)
    val options = Utils.getOptions(part)
    val format = getFormat(part)
    /** TODO(): Make this user configurable? */
    val mode = SaveMode.Append
    format match {
      case "parquet" => df.write.mode(mode).options(options).parquet(part.external_reference)
      case "csv" => df.write.mode(mode).options(options).csv(part.external_reference)
      case "orc" => df.write.mode(mode).options(options).orc(part.external_reference)
      case "hive" => df.write.mode(mode).saveAsTable(part.external_reference)
      case _ => df.write.mode(mode).options(options).format(format).save(part.external_reference)
    }
  }
}
