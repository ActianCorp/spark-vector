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

import com.actian.spark_vector.vector.Vector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.Logging
import com.actian.spark_vector.writer.WriteConf

/** Add vector operations to RecordRDD. */
trait VectorOps {

  /** Wrapper class to expose Vector operations on RDDs
   *
   *  @param rdd input RDD
   */
  implicit class VectorRDDOps(rdd: RDD[Seq[Any]]) extends Logging {

    /** Load a target Vector table from this RDD.
     *
     *  @param schema Input RDD schema
     *  @param targetTable name of the table to load
     *  @param vectorProps connection properties to the Vector instance
     *  @param preSQL set of SQL statements to execute before loading begins
     *  @param postSQL set of SQL statements to run after loading completes successfully. This SQL is executed
     *                only if the load works. The load is not rolled back if executing the postSQL fails.
     *  @param fieldMap map of input field names to target columns (optional)
     *  @param createTable if true, generates and executes a SQL create table statement based on the RDD schema
     *  @return a <code>LoaderResult</code> instance which contains results of the load operation
     */
    def loadVector(
      schema: StructType,
      targetTable: String,
      vectorProps: VectorConnectionProperties,
      preSQL: Option[Seq[String]] = None,
      postSQL: Option[Seq[String]] = None,
      fieldMap: Option[Map[String, String]] = None,
      createTable: Boolean = false): Long = {
      LoadVector.loadVector(rdd, schema, targetTable, vectorProps,
        preSQL, postSQL, fieldMap, createTable)
    }

    /** Load into a Vector table from the given RDD.
     *
     *  This method should be used when the WriteConf is known ahead of type (e.g. was communicated
     *  through a separate channel)
     *  @param schema RDD schema
     *  @param table Vector table to load to
     *  @param tableColumnMetadata A list of ColumnMetadata structures, one for each column of the given table describing
     *  the schema of the Vector table
     *  @param writeConf Write configuration to be used to connect to the `DataStream` API
     */
    def loadVector(
      schema: StructType,
      table: String,
      tableColumnMetadata: Seq[ColumnMetadata],
      writeConf: WriteConf) = LoadVector.loadVector(rdd, schema, table, tableColumnMetadata, writeConf)
  }
}

object VectorOps extends VectorOps
