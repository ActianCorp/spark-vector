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
import org.apache.spark.sql.{ Row, DataFrame, SQLContext, sources }
import org.apache.spark.sql.sources.{ BaseRelation, Filter, InsertableRelation, PrunedFilteredScan }
import org.apache.spark.sql.types.StructType
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.InternalRow

import com.actian.spark_vector.util.{ RDDUtil, ResourceUtil }
import com.actian.spark_vector.vector.{ VectorJDBC, VectorOps, ColumnMetadata }

private[spark_vector] class VectorRelation(tableRef: TableRef,
    userSpecifiedSchema: Option[StructType],
    override val sqlContext: SQLContext,
    options: Map[String, String]) extends BaseRelation with InsertableRelation with PrunedFilteredScan with Logging {
  import VectorRelation._
  import VectorOps._

  private lazy val tableMetadataSchema = getTableSchema(tableRef)
  override def schema: StructType = userSpecifiedSchema.getOrElse(structType(tableMetadataSchema))
  override def needConversion: Boolean = false

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if (overwrite) {
      VectorJDBC.withJDBC(tableRef.toConnectionProps) { _.executeStatement(s"delete from ${quote(tableRef.table)}") }
    }

    logInfo(s"Insert rdd '${data}' into Vector table '${tableRef.table}'")
    val anySeqRDD = data.rdd.map(_.toSeq)
    val preSQL = getSQL(LoadPreSQL, options)
    val postSQL = getSQL(LoadPostSQL, options)
    /** TODO: Could expose other options in Spark parameters */
    val rowCount = anySeqRDD.loadVector(data.schema, tableRef.toConnectionProps, tableRef.table, Some(preSQL), Some(postSQL))
    logInfo(s"Loaded ${rowCount} records into table ${tableRef.table}")
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val (whereClause, whereParams) = VectorRelation.generateWhereClause(filters)
    if (requiredColumns.isEmpty) {
      val countQuery = s"select count(*) from ${quote(tableRef.table)} ${whereClause}"
      logInfo(s"Required columns is empty, execute Vector prepared count query: ${countQuery}")
      VectorJDBC.withJDBC(tableRef.toConnectionProps) {
        _.executePreparedQuery[RDD[Row]](countQuery, whereParams) { rs =>
          if (!rs.next()) {
            throw new IllegalStateException(s"Could not read the result set of '${countQuery}'.")
          }
          val numRows = rs.getLong(1)
          logDebug(s"Create empty RDD from the count(*) of ${numRows} row(s).")
          sqlContext.sparkContext.parallelize(1L to numRows).map(_ => InternalRow.empty).asInstanceOf[RDD[Row]]
        }
      }
    } else {
      val (selectColumns, selectTableMetadata) = (requiredColumns.mkString(","), pruneColumns(requiredColumns, tableMetadataSchema))
      logInfo(s"Execute Vector prepared query: select ${selectColumns} from ${tableRef.table} ${whereClause}")
      sqlContext.sparkContext.unloadVector(tableRef.toConnectionProps, tableRef.table, selectTableMetadata,
        selectColumns, whereClause, whereParams)
    }
  }
}

object VectorRelation {
  final val LoadPreSQL = "loadpresql"
  final val LoadPostSQL = "loadpostsql"

  def apply(tableRef: TableRef, userSpecifiedSchema: Option[StructType], sqlContext: SQLContext, options: Map[String, String]): VectorRelation =
    new VectorRelation(tableRef, userSpecifiedSchema, sqlContext, options)

  def apply(tableRef: TableRef, sqlContext: SQLContext, options: Map[String, String]): VectorRelation =
    new VectorRelation(tableRef, None, sqlContext, options)

  /** Obtain the metadata containing the schema for the table referred by tableRef */
  def getTableSchema(tableRef: TableRef): Seq[ColumnMetadata] =
    VectorJDBC.withJDBC(tableRef.toConnectionProps) { _.columnMetadata(tableRef.table) }

  private def structType(tableMetadataSchema: Seq[ColumnMetadata]): StructType =
    StructType(tableMetadataSchema.map(_.structField))

  /** Obtain the structType containing the schema for the table referred by tableRef */
  def structType(tableRef: TableRef): StructType = structType(getTableSchema(tableRef))

  /** Retrieves a series of SQL queries from the option with key `key` */
  def getSQL(key: String, options: Map[String, String]): Seq[String] =
    options.filterKeys(_.toLowerCase startsWith key).toSeq.sortBy(_._1).map(_._2)

  /** Quote the column name so that it can be used in VectorSQL statements */
  def quote(name: String): String = name.split("\\.").map("\"" + _ + "\"").mkString(".")

  /**
   * Converts a Filter structure into an equivalent prepared Vector SQL statement that can be
   * used directly with Vector jdbc. The parameters are stored in the second element of the
   * returned tuple
   */
  def convertFilter(filter: Filter): (String, Seq[Any]) = filter match {
    case sources.EqualTo(attribute, value) => (s"${quote(attribute)} = ?", Seq(value))
    case sources.LessThan(attribute, value) => (s"${quote(attribute)} < ?", Seq(value))
    case sources.LessThanOrEqual(attribute, value) => (s"${quote(attribute)} <= ?", Seq(value))
    case sources.GreaterThan(attribute, value) => (s"${quote(attribute)} > ?", Seq(value))
    case sources.GreaterThanOrEqual(attribute, value) => (s"${quote(attribute)} >= ?", Seq(value))
    case sources.In(attribute, values) => (quote(attribute) + " IN " + values.map(_ => "?").mkString("(", ", ", ")"), values.toSeq)
    case _ => throw new UnsupportedOperationException(
      s"It's not a valid filter $filter to be pushed down, only >, <, >=, <= and In are allowed.")
  }

  /**
   * Given a sequence of filters, generate the where clause that can be used in the prepared statement
   * together with its parameters
   */
  def generateWhereClause(filters: Array[Filter]): (String, Seq[Any]) = {
    val convertedFilters = filters.map(convertFilter)
    if (!convertedFilters.isEmpty) {
      (convertedFilters.map(_._1).mkString("where ", " and ", ""), convertedFilters.flatMap(_._2))
    } else {
      ("", Nil)
    }
  }

  /** Selects the subset of columns (represented by `ColumnMetadata` structures) as required by `selectColumns` */
  def pruneColumns(selectColumns: Array[String], columnMetadata: Seq[ColumnMetadata]): Seq[ColumnMetadata] = 
    selectColumns.map(col => columnMetadata.find(_.name.equalsIgnoreCase(col)).getOrElse(
      throw new IllegalArgumentException(s"Column '${col}' not found in table metadata schema.")))
}
