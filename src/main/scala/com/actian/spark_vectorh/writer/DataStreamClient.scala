package com.actian.spark_vectorh.writer

import java.sql.SQLException
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import org.apache.spark.Logging
import com.actian.spark_vectorh.util.ResourceUtil.closeResourceOnFailure
import com.actian.spark_vectorh.vector.{ VectorConnectionProperties, VectorJDBC }
import com.actian.spark_vectorh.vector.VectorException
import com.actian.spark_vectorh.vector.ErrorCodes

/**
 * A client for to prepare loading and issue the load `SQL` query to Vector
 *
 * @param vectorProps connection information
 * @param table to which table this client will load data
 */
case class DataStreamClient(vectorProps: VectorConnectionProperties,
    table: String) extends Serializable with Logging {
  private lazy val jdbc = {
    val ret = new VectorJDBC(vectorProps)
    ret.autoCommit(false)
    ret
  }

  private def startLoadSql(table: String) = s"copy table $table from external"
  private def prepareLoadSql(table: String) = s"prepare for x100 copy into $table"

  /** The `JDBC` connection used by this client to communicate with `Vector(H)` */
  def getJdbc(): VectorJDBC = jdbc

  /** Abort sending data to Vector(H) rolling back the open transaction and closing the `JDBC` connection */
  def close(): Unit = {
    logDebug("Closing DataStrealClient")
    jdbc.rollback()
    jdbc.close
  }

  /** Prepare loading data to Vector(H). This step may not be necessary anymore in the future */
  def prepareDataStreams: Unit = jdbc.executeStatement(prepareLoadSql(table))

  /**
   * Obtain the information about how many `DataStream`s Vector(H) expects together with
   * locality information and authentication roles and tokens
   */
  def getWriteConf(): WriteConf = {
    val ret = WriteConf(jdbc)
    logDebug(s"Got ${ret.vectorEndPoints.length} datastreams")
    ret
  }

  /** Start loading data to Vector(H) */
  def startLoad(): Future[Int] = Future {
    val ret = try {
      closeResourceOnFailure(this) {
        jdbc.executeStatement(startLoadSql(table))
      }
    } catch {
      case e: SQLException =>
        throw new VectorException(e.getErrorCode, e.getMessage, e)
    }
    ret
  }

  /** Commit the transaction opened by this client */
  def commit: Unit = jdbc.commit
}
