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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext

import com.actian.spark_vector.util.{ RDDUtil, ResourceUtil }
import com.actian.spark_vector.vector.VectorUtil._
import com.actian.spark_vector.datastream.writer.{ DataStreamWriter, InsertRDD }
import com.actian.spark_vector.datastream.reader.{ DataStreamReader, ScanRDD }


/** Utility object that defines methods for loading data into Vector */
object Vector {
  import RDDUtil._
  import ResourceUtil._

  /**
   * Given an `rdd` with data types specified by `schema`, try to load it to the Vector table `targetTable`
   * using the connection information stored in `vectorProps`.
   *
   * @param preSQL specify some queries to be executed before loading, in the same transaction
   * @param postSQL specify some queries to be executed after loading, in the same transaction
   * @param fieldMap specify how the input `RDD` columns should be mapped to `targetTable` columns
   * @param createTable specify if the table should be created if it does not exist
   */
  def loadVector(rdd: RDD[Seq[Any]],
    rddSchema: StructType,
    targetTable: String,
    vectorProps: VectorConnectionProperties,
    preSQL: Option[Seq[String]],
    postSQL: Option[Seq[String]],
    fieldMap: Option[Map[String, String]],
    createTable: Boolean = false): Long = {
    val resolvedFieldMap = fieldMap.getOrElse(Map.empty)
    val optCreateTableSQL = Some(createTable).filter(identity).map(_ => TableSchemaGenerator.generateTableSQL(targetTable, rddSchema))
    val tableMetadataSchema = getTableSchema(vectorProps, targetTable, optCreateTableSQL)
    val tableStructTypeSchema = StructType(tableMetadataSchema.map(_.structField))

    // Apply the given field map return a sequence of field name, column name tuples
    val field2Columns = applyFieldMap(resolvedFieldMap, rddSchema, tableStructTypeSchema)
    // Validate the list of columns are OK to load
    validateColumns(tableStructTypeSchema, field2Columns.map(_.columnName))

    // If a subset of input fields are needed to load, select only the fields needed
    val (inputRDD, inputType) = if (field2Columns.length < rddSchema.fields.length) {
      selectFields(rdd, rddSchema, field2Columns.map(_.fieldName))
    } else {
      (rdd, rddSchema)
    }
    val finalRDD = fillWithNulls(inputRDD, inputType, tableStructTypeSchema,
      field2Columns.map(i => i.columnName -> i.fieldName).toMap)

    val writer = new DataStreamWriter[Seq[Any]](vectorProps, targetTable, tableMetadataSchema)
    closeResourceOnFailure(writer.client) {
      preSQL.foreach(_.foreach(writer.client.getJdbc.executeStatement))
    }

    val insertRDD = new InsertRDD(finalRDD, writer)
    val result = writer.initiateLoad
    insertRDD.sparkContext.runJob(insertRDD, writer.write _)

    // FIX ME
    val rowCount = Await.result(result, Duration.Inf)
    if (rowCount >= 0) {
      // Run post-load SQL
      closeResourceOnFailure(writer.client) {
        postSQL.foreach(_.foreach(writer.client.getJdbc.executeStatement))
      }
      writer.commit
    }
    writer.client.getJdbc.close

    rowCount
  }

  def unloadVector(sparkContext: SparkContext,
    vectorProps: VectorConnectionProperties,
    targetTable: String,
    tableMetadataSchema: Seq[ColumnMetadata],
    selectColumns: String = "*",
    whereClause: String = "",
    whereParams: Seq[Any] = Seq.empty[Any]): RDD[Row] = {
    val reader = new DataStreamReader(vectorProps, targetTable, tableMetadataSchema)
    val scanRDD = new ScanRDD(sparkContext, reader)
    assert(whereClause.isEmpty == whereParams.isEmpty)
    reader.initiateUnload(s"select ${selectColumns} from ${targetTable} ${whereClause}", whereParams)
    scanRDD.asInstanceOf[RDD[Row]]
  }
}
