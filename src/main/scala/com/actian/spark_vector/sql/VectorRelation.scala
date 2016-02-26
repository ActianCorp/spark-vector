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
package com.actian.spark_vector.sql

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row, SQLContext, sources }
import org.apache.spark.sql.sources.{ BaseRelation, Filter, InsertableRelation, PrunedFilteredScan }
import org.apache.spark.sql.types.StructType

import com.actian.spark_vector.vector.VectorJDBC;

private[sql] class VectorRelation(tableRef: TableRef, userSpecifiedSchema: Option[StructType], override val sqlContext: SQLContext)
    extends BaseRelation with InsertableRelation with PrunedFilteredScan with Logging {
  import com.actian.spark_vector.vector.VectorOps._

  override def schema: StructType =
    userSpecifiedSchema.getOrElse(VectorRelation.structType(tableRef))

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if (overwrite) {
      VectorJDBC.withJDBC(tableRef.toConnectionProps) { cxn =>
        cxn.executeStatement(s"delete from ${tableRef.table}")
      }
    }

    logInfo(s"Trying to insert rdd: $data into Vector table")
    val anySeqRDD = data.rdd.map(row => row.toSeq)
    // TODO could expose other options in Spark parameters
    val rowCount = anySeqRDD.loadVector(data.schema, tableRef.table, tableRef.toConnectionProps)
    logInfo(s"loaded ${rowCount} records into table ${tableRef.table}")
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    logDebug("vector: buildScan: columns: " + requiredColumns.mkString(", "))
    logDebug("vector: buildScan: filters: " + filters.mkString(", "))

    val columns =
      if (requiredColumns.isEmpty) {
        "*"
      } else {
        requiredColumns.mkString(",")
      }

    val (whereClause, whereParams) = VectorRelation.generateWhereClause(filters)
    val statement = s"select $columns from ${tableRef.table} $whereClause"
    logDebug(s"Executing Vector select statement: $statement")
    /** TODO: replace this JDBC call with creating a RDD that reads data in parallel from `Vector` in parallel */
    val results = VectorJDBC.withJDBC(tableRef.toConnectionProps) { cxn =>
      cxn.query(statement, whereParams);
    }

    val rows = results.map(Row.fromSeq(_))
    sqlContext.sparkContext.parallelize(rows)
  }
}

object VectorRelation {

  def apply(tableRef: TableRef, userSpecifiedSchema: Option[StructType], sqlContext: SQLContext): VectorRelation = {
    new VectorRelation(tableRef, userSpecifiedSchema, sqlContext)
  }

  def apply(tableRef: TableRef, sqlContext: SQLContext): VectorRelation = {
    new VectorRelation(tableRef, None, sqlContext)
  }

  /** Obtain the structType containing the schema for the table referred by tableRef */
  def structType(tableRef: TableRef): StructType = {
    VectorJDBC.withJDBC(tableRef.toConnectionProps) { cxn =>
      val structFields = cxn.columnMetadata(tableRef.table).map(_.structField)
      StructType(structFields)
    }
  }

  /** Quote the column name so that it can be used in VectorSQL statements */
  def quote(name: String): String = "\"" + name + "\""

  /**
   * Converts a Filter structure into an equivalent prepared Vector SQL statement that can be
   *  used directly with Vector jdbc. The parameters are stored in the second element of the
   *  returned tuple
   */
  def convertFilter(filter: Filter): (String, Seq[Any]) = {
    filter match {
      case sources.EqualTo(attribute, value) => (s"${quote(attribute)} = ?", Seq(value))
      case sources.LessThan(attribute, value) => (s"${quote(attribute)} < ?", Seq(value))
      case sources.LessThanOrEqual(attribute, value) => (s"${quote(attribute)} <= ?", Seq(value))
      case sources.GreaterThan(attribute, value) => (s"${quote(attribute)} > ?", Seq(value))
      case sources.GreaterThanOrEqual(attribute, value) => (s"${quote(attribute)} >= ?", Seq(value))
      case sources.In(attribute, values) =>
        (quote(attribute) + " IN " + values.map(_ => "?").mkString("(", ", ", ")"), values.toSeq)
      case _ =>
        throw new UnsupportedOperationException(
          s"It's not a valid filter $filter to be pushed down, only >, <, >=, <= and In are allowed.")
    }
  }

  /**
   * Given a sequence of filters, generate the where clause that can be used in the prepared statement
   * together with its parameters
   */
  def generateWhereClause(filters: Array[Filter]): (String, Seq[Any]) = {
    val convertedFilters = filters.map(convertFilter)
    if (!convertedFilters.isEmpty)
      (convertedFilters.map(_._1).mkString("where ", " and ", ""), convertedFilters.flatMap(_._2))
    else
      ("", Nil)
  }
}
