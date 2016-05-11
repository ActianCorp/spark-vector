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

import org.apache.spark.sql.types._
import org.scalacheck.Gen.identifier
import org.scalacheck.Shrink
import org.scalatest.{ FunSuite, Inspectors, Matchers }
import org.scalatest.prop.PropertyChecks

import com.actian.spark_vector.vector.VectorJDBC.withJDBC;
import com.actian.spark_vector.DataTypeGens.schemaGen
import com.actian.spark_vector.test.IntegrationTest
import com.actian.spark_vector.test.tags.RandomizedTest

@IntegrationTest
class TableSchemaGeneratorTest extends FunSuite with Matchers with PropertyChecks with VectorFixture {
  import com.actian.spark_vector.DataTypeGens._
  import com.actian.spark_vector.vector.TableSchemaGenerator._
  import org.scalacheck.Gen._

  val defaultFields: Seq[StructField] = Seq(
    StructField("a", BooleanType, true),
    StructField("b", ByteType, false),
    StructField("c", ShortType, true),
    StructField("d", IntegerType, false),
    StructField("e", LongType, true),
    StructField("f", FloatType, false),
    StructField("g", DoubleType, true),
    StructField("h", DecimalType(10, 2), false),
    StructField("i", DateType, true),
    StructField("j", TimestampType, false),
    StructField("k", StringType, true))

  val defaultSchema = StructType(defaultFields)

  test("table schema") {
    withJDBC(connectionProps)(cxn => {
      cxn.autoCommit(false)
      assertSchemaGeneration(cxn, "testtable", defaultSchema)
    })
  }

  test("table schema/gen", RandomizedTest) {
    withJDBC(connectionProps)(cxn => {
      cxn.autoCommit(false)
      forAll(identifier, schemaGen)((name, schema) => {
        assertSchemaGeneration(cxn, name, schema)
      })(PropertyCheckConfig(minSuccessful = 5), Shrink.shrinkAny[String], Shrink.shrinkAny[StructType])
    })
  }

  private def assertSchemaGeneration(cxn: VectorJDBC, name: String, schema: StructType): Unit = {
    val sql = generateTableSQL(name, schema)
    try {
      cxn.executeStatement(sql)
      val columnsAsFields = cxn.columnMetadata(name).map(_.structField)
      columnsAsFields.size should be(schema.fields.length)
      Inspectors.forAll(columnsAsFields.zip(schema.fields)) {
        case (columnField, origField) => {
          columnField.name should be(origField.name.toLowerCase)
          columnField.dataType should be(origField.dataType)
          columnField.nullable should be(origField.nullable)
          // TODO ensure field metadata consistency
        }
      }
      cxn.dropTable(name)
    } finally {
      cxn.rollback()
    }
  }
}
