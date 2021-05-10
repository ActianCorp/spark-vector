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
 *
 * Maintainer: francis.gropengieser@actian.com
 */
package com.actian.spark_vector.vector

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * Test utilities
 */
trait VectorFixture {
  def connectionProps: VectorConnectionProperties = {
    val host = System.getProperty("vector.host", "")
    val instance = System.getProperty("vector.instance", "")
    val jdbcPort = System.getProperty("vector.jdbcPort", "")
    val instanceOffset = System.getProperty("vector.instanceOffset", if (jdbcPort.isEmpty) JDBCPort.defaultInstanceOffset else "")
    val database = System.getProperty("vector.database", "")
    val user = System.getProperty("vector.user", "")
    val password = System.getProperty("vector.password", "")

    VectorConnectionProperties(host, JDBCPort(Some(instance).filter(!_.isEmpty), Some(instanceOffset).filter(!_.isEmpty), Some(jdbcPort).filter(!_.isEmpty)),
        database, Some(user).filter(!_.isEmpty), Some(password).filter(!_.isEmpty))
  }

  def nameNodeAddr = System.getProperty("vector.namenode.address", "hdfs://hornet:8020/")

  // Generate a create table statement from the column metadata
  def createTableStatement(tableName: String, columnMD: Seq[ColumnMetadata]) = {
    val colDefs = columnMD.map(columnMD => {
      Seq[String](columnMD.name, columnMD.typeName, if (!columnMD.nullable) "not null" else "").mkString(" ")
    }).mkString("(", ", ", ")")

    val partitions = if (connectionProps.port.value.toUpperCase().contains("H")) {
      // TODO: Need to determine best way to dynamically specify partitions for tests
      " WITH NOPARTITION"
    } else {
      " WITH NOPARTITION"
    }

    s"create table $tableName $colDefs $partitions"
  }

  // Column metadata for all the Vector supported types
  def allTypesColumnMD: Seq[ColumnMetadata] = Seq(
    ColumnMetadata("c_type", "char", false, 1, 0),
    ColumnMetadata("nc_type", "nchar", true, 1, 0),
    ColumnMetadata("vc_type", "varchar", true, 1, 0),
    ColumnMetadata("nvc_type", "nvarchar", true, 1, 0),
    ColumnMetadata("tiny_int_type", "integer1", true, 3, 0),
    ColumnMetadata("small_int_type", "smallint", true, 5, 0),
    ColumnMetadata("int_type", "integer", true, 10, 0),
    ColumnMetadata("big_int_type", "bigint", true, 19, 0),
    ColumnMetadata("float_type", "float", true, 15, 0),
    ColumnMetadata("float4_type", "float4", true, 7, 0),
    ColumnMetadata("decimal_type", "decimal", true, 5, 0),
    ColumnMetadata("money_type", "money", true, 14, 2),
    ColumnMetadata("date_type", "ansidate", true, 10, 0),
    ColumnMetadata("time_tz_type", "time with time zone", true, 8, 0),
    ColumnMetadata("time_ntz_type", "time without time zone", true, 8, 0),
    ColumnMetadata("time_ltz_type", "time with local time zone", true, 8, 0),
    ColumnMetadata("ts_tz_type", "timestamp with time zone", true, 29, 6),
    ColumnMetadata("ts_ntz_type", "timestamp without time zone", true, 29, 6),
    ColumnMetadata("ts_ltz_type", "timestamp with local time zone", true, 29, 6),
    ColumnMetadata("iy2m_type", "interval year to month", true, 8, 0),
    ColumnMetadata("id2s_type", "interval day to second", true, 24, 0),
    ColumnMetadata("ipv4_type", "ipv4", true, 4, 0),
    ColumnMetadata("ipv6_type", "ipv6", true, 16, 0))

  def admitRDD(sparkContext: SparkContext): (RDD[Row], StructType) =
    createAdmitRDD(sparkContext, admitData)

  def admitRDDWithNulls(sparkContext: SparkContext): (RDD[Row], StructType) =
    createAdmitRDD(sparkContext, admitDataWithNulls)

  def admitDataStatements(): Seq[Any] =
    admitData.map(_.mkString("(", ",", ")"))

  private val admitData = Seq(
    Row(0, 0.toShort, 380.toShort, 3.61.toFloat, 3.toShort),
    Row(1, 1.toShort, 660.toShort, 3.67.toFloat, 3.toShort),
    Row(2, 1.toShort, 800.toShort, 4.0.toFloat, 1.toShort),
    Row(3, 1.toShort, 640.toShort, 3.19.toFloat, 4.toShort),
    Row(4, 0.toShort, 520.toShort, 2.93.toFloat, 4.toShort),
    Row(5, 1.toShort, 760.toShort, 3.0.toFloat, 2.toShort))

  private val admitDataWithNulls = Seq(
    Row(0, 0.toShort, 380.toShort, 3.61.toFloat, 3.toShort),
    Row(null, 1.toShort, 660.toShort, 3.67.toFloat, 3.toShort),
    Row(2, 1.toShort, 800.toShort, 4.0.toFloat, 1.toShort),
    Row(3, 1.toShort, 640.toShort, 3.19.toFloat, 4.toShort),
    Row(null, 0.toShort, 520.toShort, 2.93.toFloat, 4.toShort),
    Row(5, 1.toShort, 760.toShort, 3.0.toFloat, 2.toShort))

  private def createAdmitRDD(sparkContext: SparkContext, data: Seq[Row]): (RDD[Row], StructType) = {
    val schema = StructType(Seq(
      StructField("student_id", IntegerType),
      StructField("admit", ShortType),
      StructField("gre", ShortType),
      StructField("gpa", FloatType),
      StructField("rank", ShortType)))

    (sparkContext.parallelize(data, 2), schema)
  }
}

object VectorFixture extends VectorFixture {
  /**
   * Creates a unique table name and invokes the given function to create the table.
   * The given operation is then invoked. The created table will always be dropped
   * after invoking the user operation.
   *
   * @param createFunc a function that creates a table with the given name
   * @param op the user operation to call that uses the table
   */
  def withTable(createFunc: String => Unit)(op: String => Unit): Unit = {
    // Create table with generated, unique name
    val tmpTableName = generateTableName("unittest_table_")
    createFunc(tmpTableName)

    try {
      // Invoke user function
      op(tmpTableName)
    } finally {
      VectorJDBC.withJDBC(connectionProps) { cxn =>
        cxn.dropTable(tmpTableName);
      }
    }
  }

  private val dateFormatter = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss_SSS")

  private def generateTableName(prefix: String): String = prefix + dateFormatter.format(new Date())
}
