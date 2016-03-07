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
package com.actian.spark_vector.vector

import java.sql.{ Connection, DriverManager, PreparedStatement, ResultSet, Statement }
import java.util.Properties

import org.apache.spark.Logging
import org.apache.spark.sql.Row

import com.actian.spark_vector.Profiling
import com.actian.spark_vector.util.ResourceUtil.RichExtractableManagedResource
import com.actian.spark_vector.vector.ErrorCodes.{ InvalidDataType, NoSuchTable, SqlException, SqlExecutionError }

import resource.managed

/** Iterator over an ResultSet */
abstract class ResultSetIterator[T](result: ResultSet) extends Iterator[T] {
  def extractor: ResultSet => T
  override def hasNext: Boolean = result.next()
  override def next(): T = extractor(result)
}

object ResultSetIterator {
  def apply[T](result: ResultSet)(ex: ResultSet => T): ResultSetIterator[T] = new ResultSetIterator[T](result) {
    def extractor = ex
  }
}

/** Extracts `Rows` out of a `ResultSet` */
class ResultSetToRowIterator(result: ResultSet) extends ResultSetIterator[Row](result) with Logging with Profiling {
  lazy val numColumns = result.getMetaData.getColumnCount
  lazy val row = collection.mutable.IndexedSeq.fill[Any](numColumns)(null)
  implicit lazy val prof_accs = profileInit("resultSet extraction")
  var col = 0
  def extractor: ResultSet => Row = { rs =>
    profile("resultSet extraction")
    col = 0
    while (col < numColumns) {
      row(col) = result.getObject(col + 1)
      col += 1
    }
    val ret = Row.fromSeq(row)
    profileEnd
    ret
  }
}

/**
 * Encapsulate functions for accessing Vector using JDBC
 */
class VectorJDBC(cxnProps: VectorConnectionProperties) extends Logging {

  import resource._
  import VectorJDBC._
  import com.actian.spark_vector.sql.VectorRelation.quote

  private implicit class SparkPreparedStatement(statement: PreparedStatement) {
    def setParams(params: Seq[Any]): PreparedStatement = {
      (1 to params.size).foreach { idx =>
        val param = params(idx - 1)
        param match {
          case x: Int => statement.setInt(idx, x)
          case x: Short => statement.setShort(idx, x)
          case x: Byte => statement.setByte(idx, x)
          case x: Long => statement.setLong(idx, x)
          case x: Float => statement.setFloat(idx, x)
          case x: Double => statement.setDouble(idx, x)
          case x: java.math.BigDecimal => statement.setBigDecimal(idx, x)
          case x: String => statement.setString(idx, x)
          case x: Boolean => statement.setBoolean(idx, x)
          case x: java.sql.Timestamp => statement.setTimestamp(idx, x)
          case x: java.sql.Date => statement.setDate(idx, x)
          case _ => throw new VectorException(InvalidDataType, "Unexpected parameter type for preparing statement")
        }
      }
      statement
    }
  }

  private val dbCxn: Connection = {
    val props = new Properties()
    cxnProps.user.foreach(props.setProperty("user", _))
    cxnProps.password.foreach(props.setProperty("password", _))
    DriverManager.getConnection(cxnProps.toJdbcUrl, props)
  }

  /** Execute a `JDBC` statement closing resources on failures, using scala-arm's `resource` package */
  def withStatement[T](op: Statement => T): T = {
    managed(dbCxn.createStatement()).map(op).resolve()
  }

  /** Execute a `JDBC` prepared statement closing resources on failures, using scala-arm's `resource` package */
  def withPreparedStatement[T](query: String, op: PreparedStatement => T): T = {
    managed(dbCxn.prepareStatement(query)).map(op).resolve()
  }

  /**
   * Execute a `SQL` query closing resources on failures, using scala-arm's `resource` package,
   * mapping the `ResultSet` to a new type as specified by `op`
   */
  def executeQuery[T](sql: String)(op: ResultSet => T): T =
    withStatement(statement => managed(statement.executeQuery(sql)).map(op)).resolve()

  /** Execute a SQL query, transferring closing responsibility of the `Statement` and `ResultSet` to the caller */
  def executeUnmanagedPreparedQuery(sql: String, params: Seq[Any]): (Statement, ResultSet) = {
    val stmt = dbCxn.prepareStatement(sql)
    val rs = stmt.setParams(params).executeQuery
    (stmt, rs)
  }

  /**
   * Execute a prepared `SQL` query closing resources on failures, using scala-arm's `resource` package,
   * mapping the `ResultSet` to a new type as specified by `op`
   */
  def executePreparedQuery[T](sql: String, params: Seq[Any])(op: ResultSet => T): T =
    withPreparedStatement(sql, statement => op(statement.setParams(params).executeQuery))

  /** Execute the update `SQL` query specified by `sql` */
  def executeStatement(sql: String): Int = {
    withStatement(_.executeUpdate(sql))
  }

  /** Return true if there is a table named `tableName` in Vector */
  def tableExists(tableName: String): Boolean = {
    if (!tableName.isEmpty) {
      val sql = s"SELECT COUNT(*) FROM ${quote(tableName)} WHERE 1=0"
      try {
        executeQuery(sql)(_ != null)
      } catch {
        case exc: Exception =>
          val message = exc.getLocalizedMessage // FIXME check for english text on localized message...?
          if (!message.contains("does not exist or is not owned by you")) {
            throw new VectorException(NoSuchTable, s"SQL exception encountered while checking for existence of table ${tableName}: ${message}")
          } else {
            false
          }
      }
    } else {
      false
    }
  }

  /**
   * Retrieve the `ColumnMetadata`s for table `tableName` as a sequence containing as many elements
   * as there are columns in the table. Each element contains the name, type, nullability, precision
   * and scale of its corresponding column in `tableName`
   */
  def columnMetadata(tableName: String): Seq[ColumnMetadata] = {
    val sql = s"SELECT * FROM ${quote(tableName)}  WHERE 1=0"
    try {
      executeQuery(sql)(resultSet => {
        val metaData = resultSet.getMetaData
        for (columnIndex <- 1 to metaData.getColumnCount) yield {
          new ColumnMetadata(
            metaData.getColumnName(columnIndex),
            metaData.getColumnTypeName(columnIndex),
            metaData.isNullable(columnIndex) == 1,
            metaData.getPrecision(columnIndex),
            metaData.getScale(columnIndex))
        }
      })
    } catch {
      case exc: Exception =>
        logError(s"Unable to retrieve metadata for table '${tableName}'", exc)
        throw new VectorException(NoSuchTable, s"Unable to query target table '${tableName}': ${exc.getLocalizedMessage}")
    }
  }

  /** Parse the result of a JDBC statement as at most a single value (possibly none) */
  def querySingleResult[T](sql: String): Option[T] = {
    executeQuery(sql)(resultSet => {
      if (resultSet.next()) {
        Option(resultSet.getObject(1).asInstanceOf[T])
      } else {
        None
      }
    })
  }

  /** Execute a select query on Vector and return the results as a matrix of elements */
  def query(sql: String): Seq[Seq[Any]] = executeQuery(sql)(ResultSetIterator(_)(toRow).toVector)

  def query(sql: String, params: Seq[Any]): Seq[Seq[Any]] = executePreparedQuery(sql, params)(ResultSetIterator(_)(toRow).toVector)

  /** Drop the Vector table `tableName` if it exists */
  def dropTable(tableName: String): Unit = {
    try {
      executeStatement(s"drop table if exists ${quote(tableName)}")
    } catch {
      case exc: Exception => throw new VectorException(SqlException, s"Unable to drop table '${tableName}. Got message: ${exc.getMessage}'")
    }
  }

  /** Return true if the table `tableName` is empty */
  def isTableEmpty(tableName: String): Boolean = {
    val rowCount = querySingleResult[Int](s"select count(*) from ${quote(tableName)}")
    rowCount.map(_ == 0).getOrElse(throw new VectorException(SqlException, s"No result for count on table '${tableName}'"))
  }

  /** Close the Vector `JDBC` connection */
  def close(): Unit = dbCxn.close

  /** Set auto-commit to ON/OFF for this `JDBC` connection */
  def autoCommit(value: Boolean): Unit = {
    dbCxn.setAutoCommit(value)
  }

  /** Commit the last transaction */
  def commit(): Unit = dbCxn.commit()

  /** Rollback the last transaction */
  def rollback(): Unit = dbCxn.rollback()

  /** Return the hostname of where the ingres frontend of Vector is located (usually on the leader node) */
  def getIngresHost(): String = cxnProps.host
}

object VectorJDBC extends Logging {
  import resource._

  final val DriverClassName = "com.ingres.jdbc.IngresDriver"

  def toRow(result: ResultSet): Seq[Any] = (1 to result.getMetaData.getColumnCount).map(result.getObject)

  /** Create a `VectorJDBC`, execute the statements specified by `op` and close the `JDBC` connections when finished */
  def withJDBC[T](cxnProps: VectorConnectionProperties)(op: VectorJDBC => T): T = {
    managed(new VectorJDBC(cxnProps)).map(op).resolve()
  }

  /**
   * Run the given sequence of SQL statements in order. No results are returned.
   * A failure will cause any changes to be rolled back. An empty set of statements
   * is ignored.
   *
   * @param vectorProps connection properties
   * @param statements sequence of SQL statements to execute
   */
  def executeStatements(vectorProps: VectorConnectionProperties)(statements: Seq[String]): Unit = {
    withJDBC(vectorProps) { cxn =>
      // Turn auto-commit off. Want to commit once all statements are run.
      cxn.autoCommit(false)

      // Execute each SQL statement
      statements.foreach(statement =>
        try {
          cxn.executeStatement(statement)
        } catch {
          case exc: Exception =>
            cxn.rollback()
            logError(s"error executing SQL statement: '${statement}'", exc)
            throw VectorException(SqlExecutionError, s"Error executing SQL statement: '${statement}'")
        })
      // Commit since all SQL statements ran OK
      cxn.commit()
    }
  }
}
