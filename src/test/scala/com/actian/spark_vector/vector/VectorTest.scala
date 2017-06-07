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

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ StringType, StructField, StructType }

import org.scalatest.{ FunSuite, Matchers }
import org.scalatest.prop.PropertyChecks

import com.actian.spark_vector.test.tags.IntegrationTest
import com.actian.spark_vector.test.util.StructTypeUtil
import com.actian.spark_vector.util.Logging
import com.actian.spark_vector.vector.ErrorCodes._
import com.actian.spark_vector.vector.VectorUtil.Field2Column
import com.actian.spark_vector.vector.VectorFixture.withTable

class VectorTest extends FunSuite with Matchers with PropertyChecks with VectorFixture with Logging {
  test("getTableSchema for existing table", IntegrationTest) {
    def createTable(tableName: String): Unit = {
      VectorJDBC.withJDBC(connectionProps) { cxn =>
        cxn.dropTable(tableName)
        cxn.executeStatement(createTableStatement(tableName, allTypesColumnMD))
      }
    }

    withTable(createTable) { tableName =>
      val schema = VectorUtil.getTableSchema(connectionProps, tableName).map(_.structField)

      schema should be(allTypesColumnMD.map(_.structField))
    }
  }

  test("getTableSchema for non-existing table", IntegrationTest) {
    withTable(func => Unit) { tableName =>
      intercept[VectorException] {
        VectorUtil.getTableSchema(connectionProps, tableName)
      }
    }
  }

  test("getTableSchema with bad cxn props", IntegrationTest) {
    withTable(func => Unit) { tableName =>
      val ex = intercept[VectorException] {
        VectorUtil.getTableSchema(VectorConnectionProperties("host", "instance", "db"), tableName)
      }

      ex.errorCode should be(SqlException)
    }
  }

  private def validatePath(sc: SparkContext, path: Path): Boolean = {
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(path) && fs.isDirectory(path)) true else false
  }

  test("applyFieldMap") {
    val sourceSchema = StructTypeUtil.createSchema(("a", StringType), ("b", StringType))
    val targetSchema = StructTypeUtil.createSchema(("B", StringType), ("A", StringType))
    val result = VectorUtil.applyFieldMap(Map("a" -> "A", "b" -> "B"), sourceSchema, targetSchema)

    result should be(sourceSchema.map(_.name).zip(targetSchema.reverseMap(_.name)).map { case (a: String, b: String) => Field2Column(a, b) })
  }

  test("applyFieldMap with an empty map") {
    val sourceSchema = StructTypeUtil.createSchema(("a", StringType), ("b", StringType))
    val targetSchema = StructTypeUtil.createSchema(("B", StringType), ("A", StringType))
    val result = VectorUtil.applyFieldMap(Map(), sourceSchema, targetSchema)

    result should be(sourceSchema.map(_.name).zip(targetSchema.map(_.name)).map { case (a: String, b: String) => Field2Column(a, b) })
  }

  test("applyFieldMap with an empty map and unbalanced cardinality") {
    val sourceSchema = StructTypeUtil.createSchema(("a", StringType))
    val targetSchema = StructTypeUtil.createSchema(("B", StringType), ("A", StringType))
    val ex = intercept[VectorException] {
      VectorUtil.applyFieldMap(Map(), sourceSchema, targetSchema)
    }

    ex.errorCode should be(InvalidNumberOfInputs)
  }

  test("applyFieldMap with too many inputs") {
    val sourceSchema = StructTypeUtil.createSchema(("a", StringType), ("b", StringType), ("c", StringType))
    val targetSchema = StructTypeUtil.createSchema(("B", StringType), ("A", StringType))

    val ex = intercept[VectorException] {
      VectorUtil.applyFieldMap(Map("a" -> "A", "b" -> "B", "c" -> "C"), sourceSchema, targetSchema)
    }

    ex.errorCode should be(InvalidNumberOfInputs)
  }

  test("applyFieldMap to non-existing column") {
    val sourceSchema = StructTypeUtil.createSchema(("a", StringType), ("b", StringType), ("c", StringType), ("d", StringType))
    val targetSchema = StructTypeUtil.createSchema(("B", StringType), ("A", StringType), ("C", StringType), ("E", StringType))

    val ex = intercept[VectorException] {
      VectorUtil.applyFieldMap(Map("a" -> "A", "b" -> "B", "c" -> "C", "d" -> "D"), sourceSchema, targetSchema)
    }

    ex.errorCode should be(NoSuchColumn)
  }

  test("applyFieldMap map has reference to non-existing field") {
    val sourceSchema = StructTypeUtil.createSchema(("b", StringType))
    val targetSchema = StructTypeUtil.createSchema(("A", StringType))

    val ex = intercept[VectorException] {
      VectorUtil.applyFieldMap(Map("a" -> "A"), sourceSchema, targetSchema)
    }

    ex.errorCode should be(NoSuchSourceField)
  }

  private val testColumns = StructType(Seq(
    StructField("a", StringType, false),
    StructField("b", StringType, true)))

  test("validateColumns with required column") {
    VectorUtil.validateColumns(testColumns, Seq("a"))
  }

  test("validateColumns with all columns") {
    VectorUtil.validateColumns(testColumns, Seq("a", "b"))
  }

  test("validateColumns excluding non-null column") {
    val ex = intercept[VectorException] {
      VectorUtil.validateColumns(testColumns, Seq("b"))
    }

    ex.errorCode should be(MissingNonNullColumn)
  }
}
