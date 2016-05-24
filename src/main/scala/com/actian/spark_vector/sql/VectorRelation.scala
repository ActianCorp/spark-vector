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

import com.actian.spark_vector.datastream.VectorEndpointConf
import com.actian.spark_vector.vector.{ ColumnMetadata, VectorJDBC, VectorOps }
import org.apache.spark.sql.sources.PrunedScan

private[spark_vector] class VectorRelation(tableRef: TableRef,
    userSpecifiedSchema: Option[StructType],
    override val sqlContext: SQLContext,
    options: Map[String, String]) extends BaseRelation with InsertableRelation with PrunedFilteredScan with Logging {
  import VectorRelation._
  import VectorOps._

  private lazy val tableMetadata = getTableSchema(tableRef)

  override def schema: StructType = userSpecifiedSchema.getOrElse(structType(tableMetadata))
  override def needConversion: Boolean = false

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if (overwrite) {
      VectorJDBC.withJDBC(tableRef.toConnectionProps) { _.executeStatement(s"delete from ${tableRef.table}") }
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
    val selectColumns = if (requiredColumns.isEmpty) "*" else requiredColumns.mkString(",")
    val selectTableMetadata = pruneColumns(requiredColumns, tableMetadata)
    val (whereClause, whereParams) = VectorRelation.generateWhereClause(filters)

    logInfo(s"Execute Vector prepared query: select ${selectColumns} from ${tableRef.table} where ${whereClause}")
    sqlContext.sparkContext.unloadVector(tableRef.toConnectionProps, tableRef.table, selectTableMetadata,
      selectColumns, whereClause, whereParams)
  }
}

/**
 * Relation to be used when the column information is known ahead of time.
 *
 * @param columnMetadata metadata about columns that are contained in this relation
 * @param conf Datastream configuration for reading/writing
 */
private[spark_vector] class VectorRelationWithSpecifiedSchema(columnMetadata: Seq[ColumnMetadata], conf: VectorEndpointConf, override val sqlContext: SQLContext)
    extends BaseRelation with InsertableRelation with PrunedScan with Logging {
  import VectorOps._
  import VectorRelation._

  override def schema: StructType = structType(columnMetadata)

  override def needConversion: Boolean = false

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if (overwrite) {
      throw new UnsupportedOperationException("Cannot overwrite a VectorRelation with user specified schema")
    }
    data.rdd.map(row => row.toSeq).loadVector(data.schema, columnMetadata, conf)
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val requiredColumnsMetadata = pruneColumns(requiredColumns, columnMetadata)
    sqlContext.sparkContext.unloadVector(requiredColumnsMetadata, conf)
  }
}

object VectorRelation {
  final val LoadPreSQL = "loadpresql"
  final val LoadPostSQL = "loadpostsql"

  def apply(tableRef: TableRef, userSpecifiedSchema: Option[StructType], sqlContext: SQLContext, options: Map[String, String]): VectorRelation =
    new VectorRelation(tableRef, userSpecifiedSchema, sqlContext, options)

  def apply(tableRef: TableRef, sqlContext: SQLContext, options: Map[String, String]): VectorRelation =
    new VectorRelation(tableRef, None, sqlContext, options)

  def apply(columnMetadata: Seq[ColumnMetadata], conf: VectorEndpointConf, sqlContext: SQLContext): VectorRelationWithSpecifiedSchema =
    new VectorRelationWithSpecifiedSchema(columnMetadata, conf, sqlContext)

  /** Obtain the metadata containing the schema for the table referred by tableRef */
  def getTableSchema(tableRef: TableRef): Seq[ColumnMetadata] =
    VectorJDBC.withJDBC(tableRef.toConnectionProps) { _.columnMetadata(tableRef.table, tableRef.cols) }

  def structType(tableMetadataSchema: Seq[ColumnMetadata]): StructType = StructType(tableMetadataSchema.map(_.structField))

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

  /** Selects the subset of columns (represented by `ColumnMetadata` structures) as required by `requiredColumns` */
  def pruneColumns(requiredColumns: Array[String], columnMetadata: Seq[ColumnMetadata]): Seq[ColumnMetadata] = if (requiredColumns.isEmpty) {
    columnMetadata
  } else {
    for {
      col <- requiredColumns
      colMetadata <- columnMetadata
      if col.equalsIgnoreCase(colMetadata.name)
    } yield colMetadata
  }
}
