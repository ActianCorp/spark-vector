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
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import com.actian.spark_vector.util.{ RDDUtil, ResourceUtil }
import com.actian.spark_vector.vector.Vector._
import com.actian.spark_vector.writer.{ DataStreamWriter, InsertRDD, RowWriter }
import com.actian.spark_vector.writer.WriteConf
import com.actian.spark_vector.writer.DataStreamClient

/** Utility object that defines methods for loading data into Vector */
object LoadVector extends Logging {

  import RDDUtil._
  import ResourceUtil._

  private def prepareRDD(rdd: RDD[Seq[Any]],
    rddSchema: StructType,
    targetSchema: StructType,
    fieldMap: Option[Map[String, String]] = None): RDD[Seq[Any]] = {
    val resolvedFieldMap = fieldMap.getOrElse(Map.empty)
    // Apply the given field map return a sequence of field name, column name tuples
    val field2Columns = applyFieldMap(resolvedFieldMap, rddSchema, targetSchema)

    // Validate the list of columns are OK to load
    validateColumns(targetSchema, field2Columns.map(_.columnName))

    // If a subset of input fields are needed to load, select only the fields needed
    val (inputRDD, inputType) =
      if (field2Columns.length < rddSchema.fields.length) {
        selectFields(rdd, rddSchema, field2Columns.map(_.fieldName))
      } else {
        (rdd, rddSchema)
      }
    fillWithNulls(inputRDD, inputType, targetSchema,
      field2Columns.map(i => i.columnName -> i.fieldName).toMap)
  }

  private def load(rdd: RDD[Seq[Any]], columnMetadata: Seq[ColumnMetadata], writeConf: WriteConf): Unit = {
    val insertRDD = new InsertRDD(rdd, writeConf)
    val rowWriter = RowWriter(columnMetadata)
    val writer = new DataStreamWriter[Seq[Any]](rowWriter, writeConf)
    insertRDD.sparkContext.runJob(insertRDD, writer.write _)
  }

  /** Given an `rdd` with data types specified by `schema`, try to load it to the Vector table `targetTable`
   *  using the connection information stored in `vectorProps`.
   *
   *  @param preSQL specify some queries to be executed before loading, in the same transaction
   *  @param postSQL specify some queries to be executed after loading, in the same transaction
   *  @param fieldMap specify how the input `RDD` columns should be mapped to `targetTable` columns
   *  @param createTable specify if the table should be created if it does not exist
   */
  def loadVector(rdd: RDD[Seq[Any]],
    rddSchema: StructType,
    table: String,
    vectorProps: VectorConnectionProperties,
    preSQL: Option[Seq[String]],
    postSQL: Option[Seq[String]],
    fieldMap: Option[Map[String, String]],
    createTable: Boolean = false): Long = {
    val optCreateTableSQL = Some(createTable).filter(identity).map(_ => TableSchemaGenerator.generateTableSQL(table, rddSchema))
    val tableColumnMetadata = getTableSchema(vectorProps, table, optCreateTableSQL)
    val tableSchema = StructType(tableColumnMetadata.map(_.structField))

    val inputRDD = prepareRDD(rdd, rddSchema, tableSchema, fieldMap)

    val client = DataStreamClient(vectorProps, table)
    val writeConf = client.getWriteConf
    closeResourceOnFailure(client) {
      preSQL.foreach(_.foreach(client.getJdbc.executeStatement))
    }
    val result = client.startLoad
    load(inputRDD, tableColumnMetadata, writeConf)
    val rowCount = Await.result(result, Duration.Inf)
    // FIX ME
    if (rowCount >= 0) {
      // Run post-load SQL
      closeResourceOnFailure(client) {
        postSQL.foreach(_.foreach(client.getJdbc.executeStatement))
        client.commit
      }
    }
    client.getJdbc.close

    rowCount
  }

  def loadVector(rdd: RDD[Seq[Any]], rddSchema: StructType, table: String, tableColumnMetadata: Seq[ColumnMetadata], writeConf: WriteConf): Unit = {
    val tableSchema = StructType(tableColumnMetadata.map(_.structField))
    val inputRDD = prepareRDD(rdd, rddSchema, tableSchema)
    load(inputRDD, tableColumnMetadata, writeConf)
  }
}
