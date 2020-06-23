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

  val csvPath: String = "../testdata/employees.csv"

  override protected def withFixture(test: OneArgTest): Outcome = {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("external table test")
    managed(SparkSession.builder.config(conf).getOrCreate()).acquireAndGet { spark =>
      withFixture(test.toNoArgTest(spark))
    }
  }

  private def csvDataFrame(schemaSpec: Option[String], header: Boolean, columnInfos: Option[Seq[ColumnInfo]])(implicit spark: SparkSession): DataFrame = {
    val opts: Map[String, String] = if(header) Map("header" -> "true") else Map.empty
    externalTableDataFrame(spark, csvPath, "csv", schemaSpec, opts, Seq.empty, columnInfos)
  }

  test("csv, no schema, no header, no column infos") { implicit spark =>
    val df = csvDataFrame(None, false, None)
    val fields = df.schema.fields
    fields.length should be (2)
    forAll(fields) { f =>
      f.dataType should be (StringType)
      f.nullable should be (true)
    }
  }

  test("csv, no schema, with header, no column infos") { implicit spark =>
    val df = csvDataFrame(None, true, None)
    df.schema.fields should be (
      Seq(
        StructField("name", StringType, true),
        StructField("salary", IntegerType, true)
      )
    )
  }

  test("csv, nullable schema, no header, no column infos") { implicit spark =>
    val df = csvDataFrame(Some("name STRING, salary INT"), false, None)
    df.schema.fields should be (
      Seq(
        StructField("name", StringType, true),
        StructField("salary", IntegerType, true)
      )
    )
  }

  test("csv, nullable schema, with header, no column infos") { implicit spark =>
    val df = csvDataFrame(Some("name STRING, salary INT"), true, None)
    df.schema.fields should be (
      Seq(
        StructField("name", StringType, true),
        StructField("salary", IntegerType, true)
      )
    )
  }

  test("csv, nullable schema, incompatible with header, no column infos") { implicit spark =>
    val df = csvDataFrame(Some("foo INT, bar STRING"), true, None)
    df.schema.fields should be (
      Seq(
        StructField("foo", IntegerType, true),
        StructField("bar", StringType, true)
      )
    )
  }

  test("csv, non-nullable schema, no header, no column infos") { implicit spark =>
    val df = csvDataFrame(Some("name STRING NOT NULL, salary INT NOT NULL"), false, None)
    df.schema.fields should be (
      Seq(
        StructField("name", StringType, false),
        StructField("salary", IntegerType, false)
      )
    )
  }

  test("csv, no schema, with header, with column infos") { implicit spark =>
    val columnInfos = Seq(new ColumnInfo("name", new LogicalType("char", 25, 25), "StringType", false, None),
                          new ColumnInfo("salary", new LogicalType("integer", 25, 25), "IntegerType", false, None) )
    val df = csvDataFrame(None, true, Some(columnInfos))
    df.schema.fields should be (
      Seq(
        StructField("name", StringType, false),
        StructField("salary", IntegerType, false)
      )
    )
  }
}
