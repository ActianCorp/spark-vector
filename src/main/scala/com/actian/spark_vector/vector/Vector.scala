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

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{ SparkListener, SparkListenerJobEnd }
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import com.actian.spark_vector.datastream.{ DataStreamClient, VectorEndpointConf }
import com.actian.spark_vector.datastream.reader.{ DataStreamReader, ScanRDD }
import com.actian.spark_vector.datastream.writer.{ DataStreamWriter, InsertRDD }
import com.actian.spark_vector.util.{ Logging, RDDUtil, ResourceUtil }
import com.actian.spark_vector.sql.VectorRelation

/** Utility object that defines methods for loading data into Vector */
private[spark_vector] object Vector extends Logging {
  import VectorUtil._
  import RDDUtil._
  import ResourceUtil._

  private def prepareRDD(rdd: RDD[Row],
    rddSchema: StructType,
    targetSchema: StructType,
    fieldMap: Option[Map[String, String]] = None): RDD[Row] = {
    val resolvedFieldMap = fieldMap.getOrElse(Map.empty)
    // Apply the given field map return a sequence of field name, column name tuples
    val field2Columns = applyFieldMap(resolvedFieldMap, rddSchema, targetSchema)

    // Validate the list of columns are OK to load
    validateColumns(targetSchema, field2Columns.map(_.columnName))

    // If a subset of input fields are needed to load, select only the fields needed
    val (inputRDD, inputType) = if (field2Columns.length < rddSchema.fields.length) {
      selectFields(rdd, rddSchema, field2Columns.map(_.fieldName))
    } else {
      (rdd, rddSchema)
    }
    val colMappingOpt = targetToInput(inputType, targetSchema, field2Columns.map(i => i.columnName -> i.fieldName).toMap)
    logDebug(s"Mapping of cols before load is $colMappingOpt for inputTypeFields = ${inputType.fields.map(_.name).mkString(",")}, targetTypeFields = ${targetSchema.fields.map(_.name).mkString(",")}")
    if (colMappingOpt.isDefined) inputRDD.map(row => Row.fromSeq(colMappingOpt.get.map { i => if (i.isDefined) row(i.get) else null })) else inputRDD
  }

  private def load(rdd: RDD[Row], columnMetadata: Seq[ColumnMetadata], writeConf: VectorEndpointConf): Unit = {
    val insertRDD = new InsertRDD(rdd, writeConf)
    val writer = new DataStreamWriter[Row](writeConf, columnMetadata)
    insertRDD.sparkContext.runJob(insertRDD, writer.write _)
  }

  /**
   * Given an `rdd` with data types specified by `rddSchema`, try to load it to the Vector table `table`
   * using the connection information stored in `vectorProps`.
   *
   * @param preSQL specify some queries to be executed before loading, in the same transaction
   * @param postSQL specify some queries to be executed after loading, in the same transaction
   * @param fieldMap specify how the input `RDD` columns should be mapped to `table` columns
   * @param createTable specify if the table should be created if it does not exist
   */
  def loadVector(rdd: RDD[Row],
    rddSchema: StructType,
    table: String,
    vectorProps: VectorConnectionProperties,
    preSQL: Option[Seq[String]],
    postSQL: Option[Seq[String]],
    fieldMap: Option[Map[String, String]],
    createTable: Boolean = false): Long = {
    val client = new DataStreamClient(vectorProps, table)
    closeResourceAfterUse(client) {
      val optCreateTableSQL = Some(createTable).filter(identity).map(_ => TableSchemaGenerator.generateTableSQL(table, rddSchema))
      val tableColumnMetadata = getTableSchema(client.getJdbc, table, optCreateTableSQL)
      val tableSchema = StructType(tableColumnMetadata.map(_.structField))

      val inputRDD = prepareRDD(rdd, rddSchema, tableSchema, fieldMap)

      preSQL.foreach(_.foreach(client.getJdbc.executeStatement))

      client.prepareLoadDataStreams
      val writeConf = client.getVectorEndpointConf
      val result = client.startLoad
      load(inputRDD, tableColumnMetadata, writeConf)
      val rowCount = Await.result(result, Duration.Inf) // FIX ME
      if (rowCount >= 0) {
        logDebug(s"""Executing postSQL queries: ${postSQL.mkString(",")}""")
        postSQL.foreach(_.foreach(client.getJdbc.executeStatement))
        client.commit
      }
      rowCount
    }
  }

  /**
   * Given an `rdd` with data types specified by `rddSchema`, try to load it directly (without any SQL connection) to the Vector table `table`
   *
   * @param tableColumnMetadata the expected table column data type information
   * @param writeConf datastream configuration for writing
   */
  def loadVector(rdd: RDD[Row], rddSchema: StructType, tableColumnMetadata: Seq[ColumnMetadata], writeConf: VectorEndpointConf): Unit = {
    val tableSchema = VectorRelation.structType(tableColumnMetadata)
    val inputRDD = prepareRDD(rdd, rddSchema, tableSchema)
    load(inputRDD, tableColumnMetadata, writeConf)
  }

  /**
   * Given a `Spark Context` try to unload a Vector table (name is omitted since it isn't needed).
   *
   * @note We need a `SQL Context` first with a `DataFrame` generated for the select query
   *
   * @param sc spark context
   * @param tableColumnMetadata column type information for the unloaded table
   * @param readConf datastream configuration for reading
   *
   * @return an <code>RDD[Row]</code> for the unload operation
   */
  def unloadVector(sc: SparkContext, tableColumnMetadata: Seq[ColumnMetadata], readConf: VectorEndpointConf): RDD[InternalRow] = {
    val reader = new DataStreamReader(readConf, tableColumnMetadata)
    val scanRDD = new ScanRDD(sc, readConf, reader.read _)
    scanRDD
  }

  /**
   * Given a `Spark Context` try to unload the Vector table `table` using the connection
   * information stored in `vectorProps`.
   * @note We need a `SQL Context` first with a `DataFrame` generated for the select query
   *
   * @param sc spark context
   * @param table name of the table to unload
   * @param vectorPros connection properties to the Vector instance
   * @param tableColumnMetadata sequence of `ColumnMetadata` obtained for `table`
   * @param selectColumns string of select columns separated by comma
   * @param whereClause prepared string of a where clause
   * @param whereParams sequence of values for the prepared where clause
   *
   * @return an <code>RDD[Row]</code> for the unload operation
   */
  def unloadVector(sc: SparkContext,
    table: String,
    vectorProps: VectorConnectionProperties,
    tableColumnMetadata: Seq[ColumnMetadata],
    selectColumns: String = "*",
    whereClause: String = "",
    whereParams: Seq[Any] = Nil): RDD[InternalRow] = {
    val client = new DataStreamClient(vectorProps, table)
    closeResourceOnFailure(client) {
      client.prepareUnloadDataStreams
      val readConf = client.getVectorEndpointConf
      val scanRDD = unloadVector(sc, tableColumnMetadata, readConf)
      assert(whereClause.isEmpty == whereParams.isEmpty)
      var selectQuery = s"select ${selectColumns} from ${table} ${whereClause}"
      client.startUnload(selectQuery, whereParams)
      sc.addSparkListener(new SparkListener() {
        private var ended = false
        override def onJobEnd(job: SparkListenerJobEnd) = if (!ended) {
          client.close
          ended = true
          logDebug(s"Unload job ended @ ${job.time}.")
        }
      })
      scanRDD
    }
  }
}
