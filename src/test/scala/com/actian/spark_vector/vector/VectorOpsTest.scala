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

import org.apache.spark.SparkException
import org.apache.spark.sql.types.{ BooleanType, DateType, DecimalType, DoubleType, IntegerType, ShortType, StringType, StructField, StructType, TimestampType }
import org.apache.spark.sql.SQLContext
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.unsafe.types.UTF8String.{ fromString => toUTF8 }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.Filter

import org.scalacheck.Gen
import org.scalatest.{ Inspectors, Matchers, fixture }
import org.scalatest.prop.PropertyChecks

import com.actian.spark_vector.{ DataGens, Profiling, RDDFixtures, SparkContextFixture }
import com.actian.spark_vector.test.IntegrationTest
import com.actian.spark_vector.test.tags.RandomizedTest
import com.actian.spark_vector.test.util.StructTypeUtil
import com.actian.spark_vector.util.{ Logging, RDDUtil }
import com.actian.spark_vector.vector.ErrorCodes._
import com.actian.spark_vector.vector.VectorFixture.withTable
import com.actian.spark_vector.vector.VectorOps._
import com.actian.spark_vector.sql.{ TableRef, VectorRelation }
import com.actian.spark_vector.colbuffer.util.MillisecondsInDay

/** Test VectorOps */
@IntegrationTest
class VectorOpsTest extends fixture.FunSuite with SparkContextFixture with Matchers with PropertyChecks with RDDFixtures
    with VectorFixture with Logging with Profiling {
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
    |) WITH PARTITION = (HASH ON a_rank 4 PARTITIONS)""".stripMargin)
    }
  }

  def createLoadAdmitTable(tableName: String): Unit = {
    VectorJDBC.withJDBC(connectionProps) { cxn =>
      cxn.dropTable(tableName)
      cxn.executeStatement(
        s"""|create table ${tableName} (
    |  a_student_id integer not null,
    |  a_admit smallint,
    |  a_gre smallint,
    |  a_gpa float4,
    |  a_rank smallint
    |) WITH PARTITION = (HASH ON a_rank 4 PARTITIONS)""".stripMargin)
    admitDataStatements().foreach(row => cxn.executeStatement(s"""insert into ${tableName} values ${row}"""))
    }
  }

  def createTestTable(tableName: String): Unit = {
    VectorJDBC.withJDBC(connectionProps) { cxn =>
      cxn.dropTable(tableName)
      cxn.executeStatement(
          s"""|create table ${tableName}
              |as select a.table_name, a.table_owner
              |from iitables a, iitables b
              |with structure=x100""".stripMargin)
    }
  }

  test("unload admission data") { sparkFixture =>
    withTable(createLoadAdmitTable) { tableName =>
      val colmetadata = VectorRelation.getTableSchema(TableRef(connectionProps, tableName))
      val (expectedrdd, schema) = admitRDD(sparkFixture.sc)
      val result = sparkFixture.sc.unloadVector(connectionProps, tableName, colmetadata)
      val data = result.collect().sortBy(r => r(0).toString()).map(_.toSeq).toSeq
      val expected = expectedrdd.collect.map(_.toSeq).toSeq
      data.equals(expected) should be(true)
    }
  }

  test("unload admission data alternate") { sparkFixture =>
    withTable(createLoadAdmitTable) { tableName =>
      val colmetadata = VectorUtil.getTableSchema(connectionProps, tableName)
      val result = sparkFixture.sc.unloadVector(connectionProps, tableName, colmetadata)
      val (expectedrdd, schema) = admitRDD(sparkFixture.sc)
      val r = result.cache().sortBy(_(0).toString())
      val data = r.collect().map(_.toSeq).toSeq
      val expected = expectedrdd.collect.map(_.toSeq).toSeq
      data.equals(expected) should be(true)
    }
  }

  test("load admission data") { sparkFixture =>
    withTable(createAdmitTable) { tableName =>
      val (rdd, schema) = admitRDD(sparkFixture.sc)
      val result = rdd.loadVector(schema, connectionProps, tableName)

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
      val result = rdd.loadVector(schema, connectionProps, tableName, fieldMap = Some(fieldMap))

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
      val result = rdd.loadVector(schema, connectionProps, tableName, preSQL = Some(preSQL))

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
      val result = rdd.loadVector(schema, connectionProps, tableName, postSQL = Some(postSQL))

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
      val result = rdd.loadVector(schema, connectionProps, tableName, preSQL = Some(preSQL), postSQL = Some(postSQL))

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
        rdd.loadVector(schema, connectionProps, tableName)
      }

      VectorJDBC.withJDBC(connectionProps) { cxn =>
        cxn.querySingleResult(s"select count(*) from $tableName") should be(Some(0))
      }
    }
  }

  test("target table does not exist") { sparkFixture =>
    val ex = intercept[VectorException] {
      val (rdd, schema) = admitRDD(sparkFixture.sc)
      rdd.loadVector(schema, connectionProps, doesNotExistTable)
    }

    ex.errorCode should be(NoSuchTable)
  }

  test("exclude non null column") { sparkFixture =>
    withTable(createAdmitTable) { tableName =>
      val ex = intercept[VectorException] {
        val (rdd, schema) = admitRDD(sparkFixture.sc)
        rdd.loadVector(schema, connectionProps, tableName, fieldMap = Some(Map("admit" -> "a_admit", "rank" -> "a_rank")))
      }

      ex.errorCode should be(MissingNonNullColumn)
    }
  }

  test("too few input fields with no field map") { sparkFixture =>
    withTable(createAdmitTable) { tableName =>
      val ex = intercept[VectorException] {
        val (rdd, schema) = admitRDD(sparkFixture.sc)
        val (rddToLoad, inputType) = RDDUtil.selectFields(rdd, schema, Seq("student_id", "admit", "gre", "gpa"))
        rddToLoad.loadVector(inputType, connectionProps, tableName)
      }

      ex.errorCode should be(InvalidNumberOfInputs)
    }
  }

  test("too many input fields with no field map") { sparkFixture =>
    withTable(createAdmitTable) { tableName =>
      val ex = intercept[VectorException] {
        val (rdd, schema) = wideRDD(sparkFixture.sc, 20)
        rdd.loadVector(schema, connectionProps, tableName)
      }

      ex.errorCode should be(InvalidNumberOfInputs)
    }
  }

  test("map to non-existing column") { sparkFixture =>
    withTable(createAdmitTable) { tableName =>
      val ex = intercept[VectorException] {
        val (rdd, schema) = admitRDD(sparkFixture.sc)
        rdd.loadVector(schema, connectionProps, tableName, fieldMap = Some(Map("admit" -> "not-a-column")))
      }

      ex.errorCode should be(NoSuchColumn)
    }
  }

  test("field map to nowhere") { sparkFixture =>
    withTable(createAdmitTable) { tableName =>
      val ex = intercept[VectorException] {
        val (rdd, schema) = admitRDD(sparkFixture.sc)
        rdd.loadVector(schema, connectionProps, tableName, fieldMap = Some(Map("foo" -> "bar")))
      }

      ex.errorCode should be(NoSuchSourceField)
    }
  }

  test("generate table") { fixture =>
    val schema = StructTypeUtil.createSchema("i" -> IntegerType, "s" -> StringType, "b" -> BooleanType)
    val data = Seq(Row(42, "foo", true))
    assertTableGeneration(fixture, schema, data, Map.empty)
  }

  test("generate table/gen", RandomizedTest) { fixture =>
    forAll(DataGens.dataGen, minSuccessful(20))(typedData => {
      val (dataType, data) = (typedData.dataType, typedData.data)
      assertTableGeneration(fixture, dataType, data, Map.empty)
    })
  }

  test("generate table/all types", RandomizedTest) { fixture =>
    forAll(DataGens.allDataGen, minSuccessful(20))(typedData => {
      val (dataType, data) = (typedData.dataType, typedData.data)
      assertTableGeneration(fixture, dataType, data, Map.empty)
    })
  }

  test("generate table/constant column") { fixture =>
    // FIXME: this is a hackish test to verify unload with a ct column (although this case is not often
    // for simple/basic queries, but more complex ones in ExternalScans) using two different user
    // defined schemas, one for load and the other for unload; the latter has c1 defined for the ct col
    val schema = StructTypeUtil.createSchema("i0" -> IntegerType)
    val data = Seq(Row(42), Row(43))
    val rdd = fixture.sc.parallelize(data)
    withTable(func => Unit) { tableName =>
      rdd.loadVector(schema, connectionProps, tableName, fieldMap = Some(Map.empty), createTable = true)
      val schemaWithCtColumn = StructTypeUtil.createSchema("i0" -> IntegerType, "si0" -> ShortType)
      val dataWithCtColumn = Seq(Seq[Any](42, 1), Seq[Any](43, 1)) // Should get back c0:42,43 (inserted) and c1:1,1 (constant expr)
      val sqlContext = fixture.spark.sqlContext
      val tableRef = TableRef(connectionProps, tableName)
      // Create the buildScan with other schema and column metadata
      val vectorRel = new VectorRelation(tableRef, Some(schemaWithCtColumn), sqlContext, Map.empty) {
        override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
          Vector.unloadVector(sqlContext.sparkContext, tableName, connectionProps, Seq(ColumnMetadata("i0", "integer4", false, 10, 0),
            ColumnMetadata("si0", "integer2", false, 5, 0)), "i0, 1").asInstanceOf[RDD[Row]]
      }
      val dataframe = sqlContext.baseRelationToDataFrame(vectorRel)
      val resultsSpark = dataframe.collect.map(_.toSeq).toSeq
      resultsSpark.sortBy(_.mkString) shouldBe dataWithCtColumn
    }
  }

  test("generate table/filtered select on strings") { fixture =>
    val schema = StructTypeUtil.createSchema("s0" -> StringType, "s1" -> StringType, "s2" -> StringType)
    val data = Seq(Row("abc", "def", "ghi"), Row("def", "ghi", "jkl"), Row("ghi", "jkl", "mno"))
    val rdd = fixture.sc.parallelize(data)
    withTable(func => Unit) { tableName =>
      rdd.loadVector(schema, connectionProps, tableName, fieldMap = Some(Map.empty), createTable = true)
      val expectedData = Seq(Seq[Any]("def", "ghi", "jkl"))
      val sqlContext = fixture.spark.sqlContext
      val tableRef = TableRef(connectionProps, tableName)
      val vectorRel = new VectorRelation(tableRef, Some(schema), sqlContext, Map.empty) {
        override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
          Vector.unloadVector(sqlContext.sparkContext, tableName, connectionProps, Seq(ColumnMetadata("s0", "varchar", false, 5, 0),
            ColumnMetadata("s1", "varchar", false, 5, 0), ColumnMetadata("s2", "varchar", false, 5, 0)),
            "*", "where s0 = ? or s2 = ?", Seq("def", "jkl")).asInstanceOf[RDD[Row]]
      }
      val dataframe = sqlContext.baseRelationToDataFrame(vectorRel)
      val resultsSpark = dataframe.collect.map(_.toSeq).toSeq
      resultsSpark shouldBe expectedData
    }
  }

  test("generate table/filtered select on a subset of columns") { fixture =>
    val schema = StructTypeUtil.createSchema("i0" -> IntegerType, "i1" -> IntegerType, "i2" -> IntegerType)
    val schemafiltered = StructTypeUtil.createSchema("i0" -> IntegerType, "i2" -> IntegerType)
    val data = Seq(Row(42, 43, 44), Row(43, 44, 45), Row(44, 45, 46))
    val rdd = fixture.sc.parallelize(data)
    withTable(func => Unit) { tableName =>
      rdd.loadVector(schema, connectionProps, tableName, fieldMap = Some(Map.empty), createTable = true)
      val expectedData = Seq(Seq[Any](44, 46))
      val sqlContext = fixture.spark.sqlContext
      val tableRef = TableRef(connectionProps, tableName)
      val vectorRel = new VectorRelation(tableRef, Some(schemafiltered), sqlContext, Map.empty) {
        override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
          Vector.unloadVector(sqlContext.sparkContext, tableName, connectionProps, Seq(ColumnMetadata("i0", "integer4", false, 10, 0),
            ColumnMetadata("i2", "integer4", false, 10, 0)), "i0, i2", "where i0 > ? and i1 > ?", Seq(43, 44)).asInstanceOf[RDD[Row]]
      }
      val dataframe = sqlContext.baseRelationToDataFrame(vectorRel)
      val resultsSpark = dataframe.collect.map(_.toSeq).toSeq
      resultsSpark shouldBe expectedData
    }
  }

  test("generate table/empty required columns, e.g. count(*)") { fixture =>
    val schema = StructTypeUtil.createSchema("i0" -> IntegerType, "i1" -> IntegerType, "i2" -> IntegerType)
    val data = Seq(Row(42, 43, 44), Row(43, 44, 45), Row(44, 45, 46))
    val rdd = fixture.sc.parallelize(data)
    withTable(func => Unit) { tableName =>
      rdd.loadVector(schema, connectionProps, tableName, fieldMap = Some(Map.empty), createTable = true)
      val sqlContext = fixture.spark.sqlContext
      val tableRef = TableRef(connectionProps, tableName)
      val vectorRel = new VectorRelation(tableRef, Some(schema), sqlContext, Map.empty)
      val dataframe = sqlContext.baseRelationToDataFrame(vectorRel)
      val resultsSpark = dataframe.collect.toSeq
      resultsSpark.length shouldBe 3
    }
  }

  test("generate table/field mapping") { fixture =>
    val schema = StructTypeUtil.createSchema("i" -> IntegerType, "s" -> StringType, "b" -> BooleanType)
    val data = Seq(Row(42, "foo", true))
    assertTableGeneration(fixture, schema, data, Map("i" -> "i", "b" -> "b"))
  }

  test("generate table/table already exists") { fixture =>
    withTable(createAdmitTable) { tableName =>
      val schema = StructType(Seq(StructField("i", IntegerType)))
      val rdd = fixture.sc.parallelize(Seq.empty[Row])
      a[VectorException] should be thrownBy {
        rdd.loadVector(schema, connectionProps, tableName, createTable = true)
      }
    }
  }

  test("generate table/invalid string") { fixture =>
    val badstrings = Array[String]("a\u0000string",
                                   "astring\uD800",
                                   "a\uD999string",
                                   "a\uDFFFstring")
    for (s: String <- badstrings) {
      val schema = StructTypeUtil.createSchema("s" -> StringType)
      val data = Seq(Row(s))
      a[Exception] should be thrownBy {
        assertTableGeneration(fixture, schema, data, Map("s" -> "s"))
      }
    }
  }

  test("generate table/decimal constancy") { fixture =>
    val schema = StructTypeUtil.createSchema("i" -> DecimalType(38, 12), "d" -> DecimalType(38, 12))
    val data = Seq(Row(new java.math.BigDecimal("123456789876543210"),
                       new java.math.BigDecimal("123456.78")))
    assertTableGeneration(fixture, schema, data, Map("i" -> "i", "d" -> "d"))
  }

  test("generate table/date constancy") { fixture =>
    val schema = StructTypeUtil.createSchema("t" -> DateType)
    var data = for (i <- -1800 to 200 by 10; j <- 0 to 11; k <- 1 to 28 by 9) yield Row(new Date(i, j, k))
    data = data ++ Seq(Row(new Date(-1899, 0, 1)), Row(new Date(8099, 11, 31)))
    assertTableGeneration(fixture, schema, data, Map("t" -> "t"))
  }

  test("generate table/string filters") { fixture =>
    val schema = StructTypeUtil.createSchema("i" -> IntegerType, "s" -> StringType)
    val data = Seq(Row(1, "abc"),
                   Row(2, "def  "),
                   Row(3, "  ghi"))
    val fieldMapping = Map("i" -> "i", "s" -> "s")
    val rdd = fixture.sc.parallelize(data)

    withTable(func => Unit) { tableName =>
      rdd.loadVector(schema, connectionProps, tableName, fieldMap = Some(fieldMapping), createTable = true)

      val sqlContext = fixture.spark.sqlContext
      val vectorRel = VectorRelation(TableRef(connectionProps, tableName), Some(schema), sqlContext, Map.empty[String, String])
      val dataframe = sqlContext.baseRelationToDataFrame(vectorRel)

      var row1 = dataframe.filter("s = 'abc'").first()
      row1(0) == 1 should be(true)

      row1 = dataframe.where("s = 'abc'").first()
      row1(0) == 1 should be(true)

      row1 = dataframe.filter(dataframe.col("s") === "abc").first()
      row1(0) == 1 should be(true)

      row1 = dataframe.filter(dataframe.col("s").like("abc")).first()
      row1(0) == 1 should be(true)

      row1 = dataframe.filter(dataframe.col("s").rlike("abc")).first()
      row1(0) == 1 should be(true)

      var row2 = dataframe.filter(dataframe.col("s").like("def%")).first()
      row2(0) == 2 should be(true)

      row2 = dataframe.filter(dataframe.col("s").contains("def")).first()
      row2(0) == 2 should be(true)

      var row3 = dataframe.filter(dataframe.col("s").like("%ghi")).first()
      row3(0) == 3 should be(true)

      row3 = dataframe.filter(dataframe.col("s").rlike("ghi")).first()
      row3(0) == 3 should be(true)

      row3 = dataframe.filter(dataframe.col("s").rlike("ghi")).first()
      row3(0) == 3 should be(true)

    }
  }

  test("generate table/string filter rlike") { fixture =>
    val schema = StructTypeUtil.createSchema("i" -> IntegerType, "s" -> StringType)
    val data = Seq(Row(1, "abc"),
                   Row(2, "def  "),
                   Row(3, "  ghi"))
    val fieldMapping = Map("i" -> "i", "s" -> "s")
    val rdd = fixture.sc.parallelize(data)

    withTable(func => Unit) { tableName =>
      rdd.loadVector(schema, connectionProps, tableName, fieldMap = Some(fieldMapping), createTable = true)

      val sqlContext = fixture.spark.sqlContext
      val vectorRel = VectorRelation(TableRef(connectionProps, tableName), Some(schema), sqlContext, Map.empty[String, String])
      val dataframe = sqlContext.baseRelationToDataFrame(vectorRel)

      var row1 = dataframe.filter(dataframe.col("s").rlike("abc")).first()
      row1(0) == 1 should be(true)

      row1 = dataframe.filter(dataframe.col("s").rlike("ghi")).first()
      row1(0) == 3 should be(true)

    }
  }

  test("null default replacement") { fixture =>
    val schema = StructTypeUtil.createSchema("col1" -> IntegerType,
        "col2" -> DoubleType,
        "col3" -> BooleanType,
        "col4" -> StringType)
    val data = Seq(Row(1, 0.1, false, "something"),
        Row(2, 1.2, true, "else"),
        Row(null, null, null, null))
    val rdd = fixture.sc.parallelize(data)
    val df = fixture.spark.sqlContext.createDataFrame(rdd, schema)
    var defaults = Map[String, Any]()
    def nulldefTable(tableName: String): Unit = { VectorJDBC.withJDBC(connectionProps) { cxn =>
      cxn.dropTable(tableName)
      cxn.executeStatement(
          s"""|create table ${tableName} (
              |  col1 int not null with default -1,
              |  col2 float not null with default 1.1,
              |  col3 boolean not null with default true,
              |  col4 varchar(20) not null with default 'empty'
              |) WITH NOPARTITION""".stripMargin)

      defaults = cxn.columnDefaults(tableName)
    }}

    withTable(nulldefTable) { tableName =>
      fixture.spark.sql(s"""CREATE TEMPORARY TABLE csv_sl_bool01_d2_v
                           USING com.actian.spark_vector.sql.DefaultSource
                           OPTIONS (
                             host "${connectionProps.host}",
                             database "${connectionProps.database}",
                             table "${tableName}",
                             user "${connectionProps.user.getOrElse("")}",
                             password "${connectionProps.password.getOrElse("")}",
                             port "${connectionProps.port}"
                           )""")

      val load1 = df.na.fill(defaults)
      val load = load1.rdd.loadVector(schema, connectionProps, tableName)
      load should be(3)

      val colmetadata = VectorUtil.getTableSchema(connectionProps, tableName)
      val results = fixture.sc.unloadVector(connectionProps, tableName, colmetadata)
      val actual = results.collect.sortBy(r => r(0).toString()).map(_.toSeq).toSeq
      val expected = Seq(Seq[Any](-1, 1.1, true, "empty"), Seq[Any](1, 0.1, false, "something"), Seq[Any](2, 1.2, true, "else"))
      actual.equals(expected) should be(true)
    }
  }

  test("intervalytm type") { fixture =>
    val schema = StructTypeUtil.createSchema("interval" -> StringType)
    val data = Seq(Row("-9999-11"), Row("1-1"), Row("10-11"), Row("9999-11"))
    val rdd = fixture.sc.parallelize(data)
    def intervalTable(tableName: String): Unit = { VectorJDBC.withJDBC(connectionProps) { cxn =>
      cxn.dropTable(tableName)
      cxn.executeStatement(
        s"""|create table ${tableName} (
    |  iytm INTERVAL YEAR TO MONTH
    |) WITH NOPARTITION""".stripMargin)
    }}

    withTable(intervalTable) { tableName =>
      val load = rdd.loadVector(schema, connectionProps, tableName, fieldMap = Some(Map("interval" -> "iytm")), createTable = false)
      load should be(4)

      val colmetadata = VectorUtil.getTableSchema(connectionProps, tableName)
      val results = fixture.sc.unloadVector(connectionProps, tableName, colmetadata)
      val actual = results.collect.sortBy(r => r(0).toString()).map(_.toSeq).toSeq
      val expected = Seq(Seq[Any]("-9999-11"),Seq[Any]("1-1"),Seq[Any]("10-11"),Seq[Any]("9999-11"))

      actual.equals(expected) should be(true)
    }
  }

  test("intervaldts type") { fixture =>
    val schema = StructTypeUtil.createSchema("interval" -> StringType)
    val data =  Seq(Row("-3652047 23:59:59.100"), Row("-1 01:01:01.010"), Row("1 12:12:12.002"), Row("3652047 23:59:59"))
    val rdd = fixture.sc.parallelize(data)
    def intervalTable(tableName: String): Unit = { VectorJDBC.withJDBC(connectionProps) { cxn =>
      cxn.dropTable(tableName)
      cxn.executeStatement(
       s"""|create table ${tableName} (
           |  idts INTERVAL DAY TO SECOND(3)
           |) WITH NOPARTITION""".stripMargin)
    }}

    withTable(intervalTable) { tableName =>
      val load = rdd.loadVector(schema, connectionProps, tableName, fieldMap = Some(Map("interval" -> "idts")), createTable = false)
      load should be(4)

      val colmetadata = VectorUtil.getTableSchema(connectionProps, tableName)
      val results = fixture.sc.unloadVector(connectionProps, tableName, colmetadata)
      val actual = results.collect.sortBy(r => r(0).toString()).map(_.toSeq).toSeq
      val expected = Seq(Seq[Any]("-1 01:01:01.010"), Seq[Any]("-3652047 23:59:59.100"),  Seq[Any]("1 12:12:12.002"), Seq[Any]("3652047 23:59:59"))

      actual.equals(expected) should be (true)
    }
  }

  test("intervaldts long type") { fixture =>
    val schema = StructTypeUtil.createSchema("interval" -> StringType)
    val data =  Seq(Row("-3652047 23:59:59.102030405"), Row("-1 01:01:01.010203456"), Row("1 12:12:12.003456700"), Row("3652047 23:59:59"))
    val rdd = fixture.sc.parallelize(data)
    def intervalTable(tableName: String): Unit = { VectorJDBC.withJDBC(connectionProps) { cxn =>
      cxn.dropTable(tableName)
      cxn.executeStatement(
       s"""|create table ${tableName} (
           |  idts INTERVAL DAY TO SECOND(9)
           |) WITH NOPARTITION""".stripMargin)
    }}

    withTable(intervalTable) { tableName =>
      val load = rdd.loadVector(schema, connectionProps, tableName, fieldMap = Some(Map("interval" -> "idts")), createTable = false)
      load should be(4)

      val colmetadata = VectorUtil.getTableSchema(connectionProps, tableName)
      val results = fixture.sc.unloadVector(connectionProps, tableName, colmetadata)
      val actual = results.collect.sortBy(r => r(0).toString()).map(_.toSeq).toSeq
      val expected = Seq(Seq[Any]("-1 01:01:01.010203456"), Seq[Any]("-3652047 23:59:59.102030405"),  Seq[Any]("1 12:12:12.003456700"), Seq[Any]("3652047 23:59:59"))

      actual.equals(expected) should be (true)
    }
  }

  test("IPv4 type") { fixture =>
    val schema = StructTypeUtil.createSchema("iptype4" -> StringType)
    val data = Seq(Row("0.0.0.0"), Row("01.02.03.04"), Row("192.168.0.1"), Row("255.255.255.255"))
    val rdd = fixture.sc.parallelize(data)
    def ipv4table(tableName: String): Unit = { VectorJDBC.withJDBC(connectionProps) { cxn =>
      cxn.dropTable(tableName)
      cxn.executeStatement(
          s"""|create table ${tableName} (
              | iptype4 IPV4
              | ) WITH NOPARTITION""".stripMargin)
    }}

    withTable(ipv4table) { tableName =>
      val load = rdd.loadVector(schema, connectionProps, tableName, fieldMap = Some(Map("iptype4" -> "iptype4")), createTable = false)
      load should be(4)

      val colmetadata = VectorUtil.getTableSchema(connectionProps, tableName)
      val results = fixture.sc.unloadVector(connectionProps, tableName, colmetadata)
      val actual = results.collect.sortBy(r => r(0).toString()).map(_.toSeq).toSeq
      val expected = Seq(Seq[Any]("0.0.0.0"), Seq[Any]("1.2.3.4"), Seq[Any]("192.168.0.1"), Seq[Any]("255.255.255.255"))

      actual.equals(expected) should be (true)
    }
  }

  test("IPv6 type") { fixture =>
    val schema = StructTypeUtil.createSchema("iptype6" -> StringType)
    val data = Seq(Row("::"), Row("::c0a8:1"), Row("2001:db8:0:1234:0:567:8:1"), Row("2001:db8::"), Row("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"))
    val rdd = fixture.sc.parallelize(data)
    def ipv6table(tableName: String): Unit = { VectorJDBC.withJDBC(connectionProps) { cxn =>
      cxn.dropTable(tableName)
      cxn.executeStatement(
          s"""|create table ${tableName} (
              | iptype6 IPV6
              | ) WITH NOPARTITION""".stripMargin)
    }}

    withTable(ipv6table) { tableName =>
      val load = rdd.loadVector(schema, connectionProps, tableName, fieldMap = Some(Map("iptype6" -> "iptype6")), createTable = false)
      load should be(5)

      val colmetadata = VectorUtil.getTableSchema(connectionProps, tableName)
      val results = fixture.sc.unloadVector(connectionProps, tableName, colmetadata)
      val actual = results.collect.sortBy(r => r(0).toString()).map(_.toSeq).toSeq
      val expected = Seq(Seq[Any]("2001:db8:0:1234:0:567:8:1"), Seq[Any]("2001:db8::"), Seq[Any]("::"), Seq[Any]("::c0a8:1"), Seq[Any]("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"))

      actual.equals(expected) should be (true)
    }
  }

  test("UUID type") { fixture =>
    val schema = StructTypeUtil.createSchema("idtype" -> StringType)
    val data = Seq(Row("2465efc5-50d9-45d5-9af7-31d07a497d84"), Row("a2bdea4a-2b7d-11e8-b467-0ed5f89f718b"), Row("cfd958c3-c1ac-470e-8bb1-62e4fdca4bb1"))
    val rdd = fixture.sc.parallelize(data)
    def uuidTable(tableName: String): Unit = { VectorJDBC.withJDBC(connectionProps) { cxn =>
      cxn.dropTable(tableName)
      cxn.executeStatement(
       s"""|create table ${tableName} (
           | idtype UUID
           | ) WITH NOPARTITION""".stripMargin)
    }}

    withTable(uuidTable) { tableName =>
      val load = rdd.loadVector(schema, connectionProps, tableName, fieldMap = Some(Map("idtype" -> "idtype")), createTable = false)
      load should be(3)

      val colmetadata = VectorUtil.getTableSchema(connectionProps, tableName)
      val results = fixture.sc.unloadVector(connectionProps, tableName, colmetadata)
      val actual = results.collect.sortBy(r => r(0).toString()).map(_.toSeq).toSeq
      val expected = Seq(Seq[Any]("2465efc5-50d9-45d5-9af7-31d07a497d84"), Seq[Any]("a2bdea4a-2b7d-11e8-b467-0ed5f89f718b"), Seq[Any]("cfd958c3-c1ac-470e-8bb1-62e4fdca4bb1"))

      actual.equals(expected) should be (true)
    }
  }

  test("dataframe reader hang") { fixture =>
    val props = new java.util.Properties()
    props.setProperty("user", connectionProps.user.getOrElse(""))
    props.setProperty("password", connectionProps.password.getOrElse(""))
    withTable(createLoadAdmitTable) { tableName =>
      val df = fixture.spark.read.vector(connectionProps, tableName, props)

      // spark only connects to 1 of the vector endpoints/partitions in this case.
      // until other datastream connections are opened and closed the jdbc 'insert into external table...'
      // statement won't return preventing closing of JDBC connection object which hangs the process.
      // Touching each datastream when onJobEnd triggers if the jdbc has not closed prevents the issue,
      // however this is a naive solution that could be more efficiently implemented if safer
      // methods to cancel the statement were available.
      df.head(1) // == df.show(1)
    }
  }

  test("RDD reader reuse") { fixture =>
    val props = new java.util.Properties()
    props.setProperty("user", connectionProps.user.getOrElse(""))
    props.setProperty("password", connectionProps.password.getOrElse(""))
    withTable(createLoadAdmitTable) { tableName =>
      val df = fixture.spark.read.vector(connectionProps, tableName, props)

      val run1 = df.rdd.collect().sortBy(r => r(0).toString()).toSeq
      val run2 = df.rdd.collect().sortBy(r => r(0).toString()).toSeq
      run1.equals(run2) should be(true)
    }
  }

   test("Vector-to-Spark-to-Vector loop") { fixture =>
    import fixture.spark.implicits._
    val props = new java.util.Properties()
    props.setProperty("user", connectionProps.user.getOrElse(""))
    props.setProperty("password", connectionProps.password.getOrElse(""))
    withTable(createLoadAdmitTable) { tableName =>
      val df = fixture.spark.read.vector(connectionProps, tableName, props)
      val dummy = df.map(r => r, df.encoder)
      withTable(createAdmitTable) { tableNameInsert =>
        dummy.write.vector(connectionProps, tableNameInsert, props)
        val re_read = fixture.spark.read.vector(connectionProps, tableNameInsert, props)
        val run1 = df.rdd.collect().sortBy(r => r(0).toString()).toSeq
        val run2 = re_read.rdd.collect().sortBy(r => r(0).toString()).toSeq
        run1.equals(run2) should be(true)
      }
    }
  }

  test("dataframe reader") { fixture =>
    val props = new java.util.Properties()
    props.setProperty("user", connectionProps.user.getOrElse(""))
    props.setProperty("password", connectionProps.password.getOrElse(""))
    withTable(createLoadAdmitTable) { tableName =>
      val df = fixture.spark.read.vector(connectionProps, tableName, props)
      val data = df.collect().sortBy(r => r(0).toString()).map(_.toSeq).toSeq
      val (expectedrdd, schema) = admitRDD(fixture.sc)
      val expected = expectedrdd.collect.map(_.toSeq).toSeq
      data.equals(expected) should be(true)
    }
  }

  test("dataframe reader alternate") { fixture =>
    val props = new java.util.Properties()
    withTable(createLoadAdmitTable) { tableName =>
      val df = fixture.spark.read.vector(connectionProps, tableName, props)
      val data = df.collect().sortBy(r => r(0).toString()).map(_.toSeq).toSeq
      val (expectedrdd, schema) = admitRDD(fixture.sc)
      val expected = expectedrdd.collect.map(_.toSeq).toSeq
      data.equals(expected) should be(true)
    }
  }

  test("dataframe writer") { fixture =>
    val (rdd, schema) = admitRDD(fixture.sc)
    val props = new java.util.Properties()
    props.setProperty("user", connectionProps.user.getOrElse(""))
    props.setProperty("password", connectionProps.password.getOrElse(""))
    withTable(createAdmitTable) { tableName =>
      val colmetadata = VectorUtil.getTableSchema(connectionProps, tableName)
      val actualschema = StructType(colmetadata.map(_.structField))
      val df = fixture.spark.createDataFrame(rdd, actualschema)
      df.write.vector(connectionProps, tableName, props)

      VectorJDBC.withJDBC(connectionProps) { cxn =>
        cxn.querySingleResult(s"select count(*) from $tableName") should be(Some(6))
        cxn.querySingleResult(s"select sum(a_admit) from $tableName") should be(Some(4))
        cxn.querySingleResult(s"select sum(a_gre) from $tableName") should be(Some(3760))
        cxn.querySingleResult(s"select sum(a_gpa) from $tableName") should be(Some(20.4f))
        cxn.querySingleResult(s"select sum(a_rank) from $tableName") should be(Some(17))
      }
    }
  }

  test("dataframe writer alternate") { fixture =>
    val (rdd, schema) = admitRDD(fixture.sc)
    val props = new java.util.Properties()
    props.setProperty("user", connectionProps.user.getOrElse(""))
    props.setProperty("password", connectionProps.password.getOrElse(""))
    withTable(createAdmitTable) { tableName =>
      val colmetadata = VectorUtil.getTableSchema(connectionProps, tableName)
      val actualschema = StructType(colmetadata.map(_.structField))
      val df = fixture.spark.createDataFrame(rdd, actualschema)
      df.write.vector(connectionProps, tableName, props)

      VectorJDBC.withJDBC(connectionProps) { cxn =>
        cxn.querySingleResult(s"select count(*) from $tableName") should be(Some(6))
        cxn.querySingleResult(s"select sum(a_admit) from $tableName") should be(Some(4))
        cxn.querySingleResult(s"select sum(a_gre) from $tableName") should be(Some(3760))
        cxn.querySingleResult(s"select sum(a_gpa) from $tableName") should be(Some(20.4f))
        cxn.querySingleResult(s"select sum(a_rank) from $tableName") should be(Some(17))
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
          rdd.loadVector(schema, connectionProps, tableName)
        } catch {
          case e: Exception =>
        }
        profileEnd
        (accs.accs("load").acc - lastTiming) should be < reasonableTimePerRun
      }
    }
  }

  // II-3509
  test("generic SQL error") { fixture =>
    withTable(createTestTable) { tableName =>
      for (i <- 0 until 10) {
        fixture.spark.sqlContext.sql(s"""CREATE TEMPORARY VIEW vector_table
                          USING com.actian.spark_vector.sql.DefaultSource
                          OPTIONS (
                          host "${connectionProps.host}",
                          port "${connectionProps.port}",
                          database "${connectionProps.database}",
                          user="${connectionProps.user.getOrElse("")}",
                          password="${connectionProps.password.getOrElse("")}",
                          table "${tableName}"
                          )""")
        val res = fixture.spark.sqlContext.sql("""select * from vector_table""")
        res.head(10)
        fixture.spark.sqlContext.sql("""DROP table vector_table""")
      }
    }
  }

test("II-6289 - count(1)") { sparkFixture =>
    val schema = StructTypeUtil.createSchema("i0" -> IntegerType, "i1" -> IntegerType, "i2" -> IntegerType)
    val data = Seq(Row(42, 43, 44), Row(43, 44, 45), Row(44, 45, 46))
    val rdd = sparkFixture.sc.parallelize(data)
    withTable(func => Unit) { tableName =>
        rdd.loadVector(schema, connectionProps, tableName, fieldMap = Some(Map.empty), createTable = true)
        sparkFixture.spark.sql(s"""CREATE TEMPORARY view ${tableName}
                                   USING com.actian.spark_vector.sql.DefaultSource
                                   OPTIONS (host "${connectionProps.host}",
                                   port "${connectionProps.port}",
                                   database "${connectionProps.database}",
                                   user="${connectionProps.user.getOrElse("")}",
                                   password="${connectionProps.password.getOrElse("")}",
                                   table "${tableName}")""")
        sparkFixture.spark.sql(s"""select count(1) from ${tableName}""").collectAsList().get(0).getLong(0) should be(3)
    }
}

  private def assertSingleValue(fixture: FixtureParam, schema: StructType, value: Any): Any = {
    val data = Seq(Row(value))
    val rdd = fixture.sc.parallelize(data)
    val tableName: String = "gentable"
    try {
      rdd.loadVector(schema, connectionProps, tableName, createTable = true)
      VectorJDBC.withJDBC(connectionProps)(cxn => {
        val result = cxn.query(s"select * from $tableName")
        result(0)(0) should be(value)
      })
    } finally {
      VectorJDBC.withJDBC(connectionProps)(_.dropTable(tableName))
    }
  }

  private def compareResults(result: Seq[Row], expectedData: Seq[Row], mappedIndices: Set[Int]) = {
    result.size should be(expectedData.size)
    if (result.size > 0) Inspectors.forAll(result) {
      case actRow =>
        expectedData.find {
          case expRow =>
            expRow.size == actRow.size &&
              (0 until expRow.size).filter(mappedIndices.contains).find {
                case i =>
                  !((actRow(i), expRow(i)) match {
                    case (null, e) => e == null
                    case (e, null) => e == null
                    case (act: Date, exp: Date) => Math.abs(act.getTime - exp.getTime) / MillisecondsInDay == 0
                    case (act: Double, exp: Double) => Math.abs(act - exp) < 1E20
                    case (act: Float, exp: Float) => Math.abs(act - exp) < 1E20.toFloat
                    case (act: java.math.BigDecimal, exp: java.math.BigDecimal) => act.compareTo(exp) == 0
                    case (act, exp) => act == exp
                  })
              }.isEmpty
        }.map(_ => true).getOrElse(false) should be(true)
    }
  }

  private def assertTableGeneration(fixture: FixtureParam, dataType: StructType, expectedData: Seq[Row],
    fieldMapping: Map[String, String]): Unit = {
    val mappedIndices = if (fieldMapping.isEmpty) {
      (0 until dataType.fields.size).toSet
    } else {
      dataType.fieldNames.zipWithIndex.filter(i => fieldMapping.contains(i._1)).map(_._2).toSet
    }

    val rdd = fixture.sc.parallelize(expectedData)
    withTable(func => Unit) { tableName =>
      rdd.loadVector(dataType, connectionProps, tableName, fieldMap = Some(fieldMapping), createTable = true)
      var resultsJDBC = VectorJDBC.withJDBC(connectionProps)(_.query(s"select * from $tableName")).map(Row.fromSeq)
      compareResults(resultsJDBC, expectedData, mappedIndices)

      val sqlContext = fixture.spark.sqlContext
      val vectorRel = VectorRelation(TableRef(connectionProps, tableName), Some(dataType), sqlContext, Map.empty[String, String])
      val dataframe = sqlContext.baseRelationToDataFrame(vectorRel)
      val resultsSpark = dataframe.collect()
      compareResults(resultsSpark, expectedData, mappedIndices)
    }
  }

  private def tmpDirPath = "/tmp"
}
