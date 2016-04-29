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

import java.sql.{ Date, Timestamp }

import org.apache.spark.{ Logging, SparkException }
import org.apache.spark.sql.types.{ BooleanType, IntegerType, StringType, StructField, StructType }
import org.scalacheck.Gen
import org.scalatest.{ Inspectors, Matchers, fixture }
import org.scalatest.prop.PropertyChecks

import com.actian.spark_vector.{ DataGens, Profiling, RDDFixtures, SparkContextFixture }
import com.actian.spark_vector.test.IntegrationTest
import com.actian.spark_vector.test.tags.RandomizedTest
import com.actian.spark_vector.test.util.StructTypeUtil
import com.actian.spark_vector.util.RDDUtil
import com.actian.spark_vector.vector.ErrorCodes._
import com.actian.spark_vector.vector.VectorFixture.withTable
import com.actian.spark_vector.vector.VectorOps.VectorRDDOps

/**
 * Test VectorOps
 */
@IntegrationTest
class VectorOpsTest extends fixture.FunSuite with SparkContextFixture with Matchers with PropertyChecks with RDDFixtures with VectorFixture with Logging with Profiling {

  private val doesNotExistTable = "this_table_does_not_exist"

  def createAdmitTable(tableName: String): Unit = {
    VectorJDBC.withJDBC(connectionProps) { cxn =>
      cxn.dropTable(tableName)
      cxn.executeStatement(
        s"""|create table ${tableName} (
    |  a_student_id integer not null,
    |  a_admit smallint,
    |  a_gre smallint,
    |  a_gpa float4,
    |  a_rank smallint
    |)""".stripMargin)
    }
  }

  test("load admission data") { sparkFixture =>

    withTable(createAdmitTable) { tableName =>
      val (rdd, schema) = admitRDD(sparkFixture.sc)
      val result = rdd.loadVector(schema, tableName, connectionProps)

      result should be(6)

      VectorJDBC.withJDBC(connectionProps) { cxn =>
        cxn.querySingleResult(s"select count(*) from $tableName") should be(Some(6))
        cxn.querySingleResult(s"select sum(a_admit) from $tableName") should be(Some(4))
        cxn.querySingleResult(s"select sum(a_gre) from $tableName") should be(Some(3760))
        cxn.querySingleResult(s"select sum(a_gpa) from $tableName") should be(Some(20.4f))
        cxn.querySingleResult(s"select sum(a_rank) from $tableName") should be(Some(17))
      }
    }
  }

  test("load subset of admission data") { sparkFixture =>

    withTable(createAdmitTable) { tableName =>
      val fieldMap = Map("student_id" -> "a_student_id", "admit" -> "a_admit", "rank" -> "a_rank")
      val (rdd, schema) = admitRDD(sparkFixture.sc)
      val result = rdd.loadVector(schema, tableName, connectionProps, fieldMap = Some(fieldMap))
      result should be(6)

      VectorJDBC.withJDBC(connectionProps) { cxn =>
        cxn.querySingleResult(s"select count(*) from $tableName") should be(Some(6))
        cxn.querySingleResult(s"select sum(a_admit) from $tableName") should be(Some(4))
        cxn.querySingleResult(s"select sum(a_gre) from $tableName") should be(None)
        cxn.querySingleResult(s"select sum(a_gpa) from $tableName") should be(None)
        cxn.querySingleResult(s"select sum(a_rank) from $tableName") should be(Some(17))
      }
    }
  }

  test("load admission data with preSQL") { sparkFixture =>

    withTable(createAdmitTable) { tableName =>
      val preSQL = Seq(s"insert into $tableName values (6, 1, 563, 3.4, 6)")
      val (rdd, schema) = admitRDD(sparkFixture.sc)
      val result = rdd.loadVector(schema, tableName, connectionProps, preSQL = Some(preSQL))

      result should be(6)

      VectorJDBC.withJDBC(connectionProps) { cxn =>
        cxn.querySingleResult(s"select count(*) from $tableName") should be(Some(7))
        cxn.querySingleResult(s"select sum(a_admit) from $tableName") should be(Some(5))
        cxn.querySingleResult(s"select sum(a_gre) from $tableName") should be(Some(4323))
        cxn.querySingleResult(s"select sum(a_gpa) from $tableName") should be(Some(23.8f))
        cxn.querySingleResult(s"select sum(a_rank) from $tableName") should be(Some(23))
      }
    }
  }

  test("load admission data with postSQL") { sparkFixture =>

    withTable(createAdmitTable) { tableName =>
      val postSQL = Seq(s"delete from $tableName where a_student_id > 0")
      val (rdd, schema) = admitRDD(sparkFixture.sc)
      val result = rdd.loadVector(schema, tableName, connectionProps, postSQL = Some(postSQL))
      result should be(6)

      VectorJDBC.withJDBC(connectionProps) { cxn =>
        cxn.querySingleResult(s"select count(*) from $tableName") should be(Some(1))
        cxn.querySingleResult(s"select sum(a_admit) from $tableName") should be(Some(0))
        cxn.querySingleResult(s"select sum(a_gre) from $tableName") should be(Some(380))
        cxn.querySingleResult(s"select sum(a_gpa) from $tableName") should be(Some(3.61f))
        cxn.querySingleResult(s"select sum(a_rank) from $tableName") should be(Some(3))
      }
    }
  }

  test("load admission data with preSQL and postSQL") { sparkFixture =>

    withTable(createAdmitTable) { tableName =>
      val preSQL = Seq(s"insert into $tableName values (6, 1, 563, 3.4, 6)")
      val postSQL = Seq(s"delete from $tableName where a_student_id > 0 and a_student_id < 6")
      val (rdd, schema) = admitRDD(sparkFixture.sc)
      val result = rdd.loadVector(schema, tableName, connectionProps, preSQL = Some(preSQL), postSQL = Some(postSQL))

      result should be(6)

      VectorJDBC.withJDBC(connectionProps) { cxn =>
        cxn.querySingleResult(s"select count(*) from $tableName") should be(Some(2))
        cxn.querySingleResult(s"select sum(a_admit) from $tableName") should be(Some(1))
        cxn.querySingleResult(s"select sum(a_gre) from $tableName") should be(Some(943))
        cxn.querySingleResult(s"select sum(a_gpa) from $tableName") should be(Some(7.01f))
        cxn.querySingleResult(s"select sum(a_rank) from $tableName") should be(Some(9))
      }
    }
  }

  test("load data with null key value and zero tolerance") { sparkFixture =>
    withTable(createAdmitTable) { tableName =>
      val ex = intercept[SparkException] {
        val (rdd, schema) = admitRDDWithNulls(sparkFixture.sc)
        rdd.loadVector(schema, tableName, connectionProps)
      }

      VectorJDBC.withJDBC(connectionProps) { cxn =>
        cxn.querySingleResult(s"select count(*) from $tableName") should be(Some(0))
      }
    }
  }

  test("target table does not exist") { sparkFixture =>
    val ex = intercept[VectorException] {
      val (rdd, schema) = admitRDD(sparkFixture.sc)
      rdd.loadVector(schema, doesNotExistTable, connectionProps)
    }

    ex.errorCode should be(NoSuchTable)
  }

  test("exclude non null column") { sparkFixture =>
    withTable(createAdmitTable) { tableName =>
      val ex = intercept[VectorException] {
        val (rdd, schema) = admitRDD(sparkFixture.sc)
        rdd.loadVector(schema, tableName, connectionProps, fieldMap = Some(Map("admit" -> "a_admit", "rank" -> "a_rank")))
      }

      ex.errorCode should be(MissingNonNullColumn)
    }
  }

  test("too few input fields with no field map") { sparkFixture =>
    withTable(createAdmitTable) { tableName =>
      val ex = intercept[VectorException] {
        val (rdd, schema) = admitRDD(sparkFixture.sc)
        val (rddToLoad, inputType) = RDDUtil.selectFields(rdd, schema, Seq("student_id", "admit", "gre", "gpa"))
        rddToLoad.loadVector(inputType, tableName, connectionProps)
      }

      ex.errorCode should be(InvalidNumberOfInputs)
    }
  }

  test("too many input fields with no field map") { sparkFixture =>
    withTable(createAdmitTable) { tableName =>
      val ex = intercept[VectorException] {
        val (rdd, schema) = wideRDD(sparkFixture.sc, 20)
        rdd.loadVector(schema, tableName, connectionProps)
      }

      ex.errorCode should be(InvalidNumberOfInputs)
    }
  }

  test("map to non-existing column") { sparkFixture =>
    withTable(createAdmitTable) { tableName =>
      val ex = intercept[VectorException] {
        val (rdd, schema) = admitRDD(sparkFixture.sc)
        rdd.loadVector(schema, tableName, connectionProps, fieldMap = Some(Map("admit" -> "not-a-column")))
      }

      ex.errorCode should be(NoSuchColumn)
    }
  }

  test("field map to nowhere") { sparkFixture =>
    withTable(createAdmitTable) { tableName =>
      val ex = intercept[VectorException] {
        val (rdd, schema) = admitRDD(sparkFixture.sc)
        rdd.loadVector(schema, tableName, connectionProps, fieldMap = Some(Map("foo" -> "bar")))
      }

      ex.errorCode should be(NoSuchSourceField)
    }
  }

  private def assertSingleValue(fixture: FixtureParam, schema: StructType, value: Any): Any = {
    val data = Seq(Seq[Any](value))
    val rdd = fixture.sc.parallelize(data)
    val tableName: String = "gentable"
    try {
      rdd.loadVector(schema, tableName, connectionProps, createTable = true)
      VectorJDBC.withJDBC(connectionProps)(cxn => {
        val result = cxn.query(s"select * from $tableName")
        result(0)(0) should be(value)
      })
    } finally {
      VectorJDBC.withJDBC(connectionProps)(_.dropTable(tableName))
    }
  }

  test("generate table") { fixture =>
    val schema = StructTypeUtil.createSchema("i" -> IntegerType, "s" -> StringType, "b" -> BooleanType)
    val data = Seq(Seq[Any](42, "foo", true))
    assertTableGeneration(fixture, schema, data, Map.empty)
  }

  test("generate table/gen", RandomizedTest) { fixture =>
    {
      forAll(DataGens.dataGen, minSuccessful(3))(typedData => {
        val (dataType, data) = (typedData.dataType, typedData.data)
        assertTableGeneration(fixture, dataType, data, Map.empty)
      })
    }
  }

  test("generate table/field mapping") { fixture =>
    val schema = StructTypeUtil.createSchema("i" -> IntegerType, "s" -> StringType, "b" -> BooleanType)
    val data = Seq(Seq[Any](42, "foo", true))
    assertTableGeneration(fixture, schema, data, Map("i" -> "i", "b" -> "b"))
  }

  private def assertTableGeneration(fixture: FixtureParam, dataType: StructType, data: Seq[Seq[Any]], fieldMapping: Map[String, String]): Unit = {
    val expectedData = data.map(_.toSeq)
    val mappedIndices = if (fieldMapping.isEmpty)
      (0 until dataType.fields.size).toSet
    else
      dataType.fieldNames.zipWithIndex.filter(fi => fieldMapping.contains(fi._1)).map(_._2).toSet

    val rdd = fixture.sc.parallelize(data)
    withTable(func => Unit) { tableName =>
      rdd.loadVector(dataType, tableName, connectionProps, fieldMap = Some(fieldMapping), createTable = true)
      VectorJDBC.withJDBC(connectionProps)(cxn => {
        val result = cxn.query(s"select * from $tableName")
        result.size should be(expectedData.size)
        Inspectors.forAll(result.sortBy(_.toString).zip(expectedData.sortBy(_.toString))) {
          case (actRow, expRow) =>
            actRow.size should be(expRow.size)
            Inspectors.forAll(actRow.zip(expRow).zipWithIndex.filter(i => mappedIndices.contains(i._2)).map(_._1)) {
              case (act: Date, exp: Date) => ()
              case (act: Timestamp, exp: Timestamp) => ()
              case (act, exp) => act should be(exp)
            }
        }
      })
    }
  }

  test("generate table/table already exists") { fixture =>
    {
      withTable(createAdmitTable) { tableName =>
        val schema = StructType(Seq(StructField("i", IntegerType)))
        val rdd = fixture.sc.parallelize(Seq.empty[Seq[Any]])
        a[VectorException] should be thrownBy {
          rdd.loadVector(schema, tableName, connectionProps, createTable = true)
        }
      }
    }
  }

  test("many runs do not slow down") { fixture =>
    withTable(createAdmitTable) { tableName =>
      import Gen._

      val (goodRdd, goodSchema) = admitRDD(fixture.sc)
      val (badRdd, badSchema) = admitRDDWithNulls(fixture.sc)
      val goodWeight = 3
      val badWeight = 1
      val numRuns = 50
      implicit val accs = profileInit("load")
      val reasonableTimePerRun = 3 * 1000000000L /* 3 seconds */
      val rddSchemaGen = Gen.frequency((goodWeight, (goodRdd, goodSchema)), (badWeight, (badRdd, badSchema)))
      for {
        i <- 0 until numRuns
        (rdd, schema) <- rddSchemaGen.sample
      } {
        val lastTiming = accs.accs("load").acc
        profile("load")
        try {
          rdd.loadVector(schema, tableName, connectionProps)
        } catch {
          case e: Exception =>
        }
        profileEnd
        (accs.accs("load").acc - lastTiming) should be < reasonableTimePerRun
      }
    }
  }

  private def tmpDirPath = "/tmp"
}
