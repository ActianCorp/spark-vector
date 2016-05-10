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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext

import com.actian.spark_vector.vector.Vector._

/** Add vector operations to RecordRDD. */
trait VectorOps {
  /**
   * Wrapper class to expose Vector operations on RDDs
   *
   * @param rdd input RDD to be loaded
   */
  implicit class VectorRDDOps(rdd: RDD[Seq[Any]]) {
    /**
     * Load a target Vector table from this RDD.
     *
     * @param schema Input RDD schema
     * @param vectorProps connection properties to the Vector instance
     * @param targetTable name of the table to load
     * @param preSQL set of SQL statements to execute before loading begins
     * @param postSQL set of SQL statements to run after loading completes successfully. This SQL is executed
     *   only if the load works. The load is not rolled back if executing the postSQL fails.
     * @param fieldMap map of input field names to target columns (optional)
     * @param createTable if true, generates and executes a SQL create table statement based on the RDD schema
     *
     * @return a <code>LoaderResult</code> instance which contains results of the load operation
     */
    def loadVector(rddSchema: StructType,
      vectorProps: VectorConnectionProperties,
      targetTable: String,
      preSQL: Option[Seq[String]] = None,
      postSQL: Option[Seq[String]] = None,
      fieldMap: Option[Map[String, String]] = None,
      createTable: Boolean = false): Long = {
      Vector.loadVector(rdd, rddSchema, vectorProps, targetTable,
        preSQL, postSQL, fieldMap, createTable)
    }
  }

  /**
   * Wrapper class to expose Vector operations on SparkContext
   *
   * @param sc sparkContext used for unloading data
   */
  implicit class VectorSparkContextOps(sc: SparkContext) {
    /**
     * Unload a target Vector table using this SparkContext.
     *
     * @param vectorPros connection properties to the Vector instance
     * @param targetTable name of the table to unload
     * @param tableMetadataSchema sequence of `ColumnMetadata` obtained for `targetTable`
     * @param selectColumns string of select columns separated by comma
     * @param whereClause prepared string of a where clause
     * @param whereParams sequence of values for the prepared where clause
     *
     * @return an <code>RDD[Row]</code> for the unload operation
     */
    def unloadVector(vectorProps: VectorConnectionProperties,
      targetTable: String,
      tableMetadataSchema: Seq[ColumnMetadata],
      selectColumns: String = "*",
      whereClause: String = "",
      whereParams: Seq[Any] = Nil): RDD[Row] = {
      Vector.unloadVector(sc, vectorProps, targetTable, tableMetadataSchema,
        selectColumns, whereClause, whereParams)
    }
  }
}

object VectorOps extends VectorOps
