package com.actian.spark_vector.provider

import scala.concurrent.Future
import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import com.actian.spark_vector.loader.command.VectorTempTable
import com.actian.spark_vector.writer.VectorEndPoint
import com.actian.spark_vector.writer.WriteConf
import com.actian.spark_vector.loader.command._
import org.apache.spark.sql.DataFrame
import com.actian.spark_vector.sql.VectorRelation
import com.actian.spark_vector.sql.TableRef
import play.api.libs.json._
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonMappingException
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.sql.DataFrame
import java.util.concurrent.atomic.AtomicLong
import com.actian.spark_vector.vector.VectorException
import scala.util.Success
import scala.util.Failure

case class RequestResult(jobId: String, result: String)

class RequestHandler(sqlContext: SQLContext) extends Logging {
  import Job._

  private val id = new AtomicLong(0L)

  private def run(json: String): Future[RequestResult] = Future {
    val job: Job = Json.fromJson[Job](Json.parse(json)).fold(errors => {
      throw new IllegalArgumentException(s"Invalid JSON receive: $json.\nThe errors are: ${JsError.toJson(errors)}")
    }, job => job)

    for {
      part <- job.parts
      format <- part.format.orElse {
        throw new IllegalArgumentException(s"All part jobs must have the format specified in their options, but query ${job.query_id}, part ${part.part_id} doesn't")
      }
    } {
      logDebug("Got new JSON request...")
      val vectorTable = register(part)
      val select = selectStatement(format, part)

      sqlContext.sql(s"insert into ${sparkQuote(vectorTable)} $select")
    }
    RequestResult(job.query_id.toString, "Success")
  }

  def handle(json: String): Unit = run(json) onComplete {
    case Success(result) => handleSuccess(result)
    case Failure(t) => handleFailure(json, t)
  }

  private def handleSuccess(result: RequestResult) = {
    logInfo(s"Job ${result.jobId} has succeeded")
  }

  private def handleFailure(json: String, cause: Throwable) = {
    logError(s"Job ${json} has failed with exception ${cause.getMessage}\n${cause.getStackTraceString}")
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
