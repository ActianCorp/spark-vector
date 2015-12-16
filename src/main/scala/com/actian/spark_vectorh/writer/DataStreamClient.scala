package com.actian.spark_vectorh.writer

import java.sql.SQLException

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import org.apache.spark.Logging

import com.actian.spark_vectorh.util.ResourceUtil.closeResourceOnFailure
import com.actian.spark_vectorh.vector.{ VectorConnectionProperties, VectorJDBC }

case class DataStreamClient(vectorProps: VectorConnectionProperties,
    table: String) extends Serializable with Logging {
  private lazy val jdbc = {
    val ret = new VectorJDBC(vectorProps)
    ret.autoCommit(false)
    ret
  }

  private def startLoadSql(table: String) = s"copy table $table from external"
  private def prepareLoadSql(table: String) = s"prepare for x100 copy into $table"

  def getJdbc(): VectorJDBC = jdbc

  def close(): Unit = {
    jdbc.rollback()
    jdbc.close
  }

  def prepareDataStreams: Unit = {
    //log.trace("Preparing data streams...")
    jdbc.executeStatement(prepareLoadSql(table))
  }

  def getWriteConf(): WriteConf = {
    val ret = WriteConf(jdbc)
    log.debug(s"Got ${ret.vectorEndPoints.length} datastreams")
    ret
  }

  def startLoad(): Future[Int] = Future {
    val ret = try {
      closeResourceOnFailure(this) {
        jdbc.executeStatement(startLoadSql(table))
      }
    } catch {
      case e: SQLException =>
        log.info(s"Vector raised exception")

        e.printStackTrace()
        -1
    }
    ret
  }

  def commit: Unit = jdbc.commit
}
