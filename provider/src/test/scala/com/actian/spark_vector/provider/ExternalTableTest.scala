package com.actian.spark_vector.provider

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.scalatest._

class ExternalTableTest extends fixture.FunSuite with Matchers with Inspectors {

  import ExternalTable._
  import resource._

  override type FixtureParam = SparkSession

  val csvPath: String = "testdata/employees.csv"

  override protected def withFixture(test: OneArgTest): Outcome = {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("external table test")
    managed(SparkSession.builder.config(conf).getOrCreate()).acquireAndGet { spark =>
      withFixture(test.toNoArgTest(spark))
    }
  }

  private def csvDataFrame(schemaSpec: Option[String], header: Boolean)(implicit spark: SparkSession): DataFrame = {
    val opts: Map[String, String] = if(header) Map("header" -> "true") else Map.empty
    externalTableDataFrame(spark, csvPath, "csv", schemaSpec, opts, Seq.empty)
  }

  test("csv, no schema, no header") { implicit spark =>
    val df = csvDataFrame(None, false)
    val fields = df.schema.fields
    fields.length should be (2)
    forAll(fields) { f =>
      f.dataType should be (StringType)
      f.nullable should be (true)
    }
  }

  test("csv, no schema, with header") { implicit spark =>
    val df = csvDataFrame(None, true)
    df.schema.fields should be (
      Seq(
        StructField("name", StringType, true),
        StructField("salary", StringType, true),
      )
    )
  }

  test("csv, nullable schema, no header") { implicit spark =>
    val df = csvDataFrame(Some("name STRING, salary INT"), false)
    df.schema.fields should be (
      Seq(
        StructField("name", StringType, true),
        StructField("salary", IntegerType, true),
      )
    )
  }

  test("csv, nullable schema, with header") { implicit spark =>
    val df = csvDataFrame(Some("name STRING, salary INT"), true)
    df.schema.fields should be (
      Seq(
        StructField("name", StringType, true),
        StructField("salary", IntegerType, true),
      )
    )
  }

  test("csv, nullable schema, incompatible with header") { implicit spark =>
    val df = csvDataFrame(Some("foo INT, bar STRING"), true)
    df.schema.fields should be (
      Seq(
        StructField("foo", IntegerType, true),
        StructField("bar", StringType, true),
      )
    )
  }

  test("csv, non-nullable schema, no header") { implicit spark =>
    val df = csvDataFrame(Some("name STRING NOT NULL, salary INT NOT NULL"), false)
    df.schema.fields should be (
      Seq(
        StructField("name", StringType, false),
        StructField("salary", IntegerType, false),
      )
    )
  }

}
