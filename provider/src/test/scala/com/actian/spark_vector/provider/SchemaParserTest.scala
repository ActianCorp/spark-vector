package com.actian.spark_vector.provider

import org.apache.spark.sql.types._
import org.scalatest._
import org.scalatest.prop._

class SchemaParserTest extends FunSuite with Matchers with TableDrivenPropertyChecks {

  import SchemaParser._

  test("parse success") {
    val scenarios =
      Table(
        ("input", "result"),
        ("name STRING", Seq(StructField("name", StringType, true))),
        ("name string", Seq(StructField("name", StringType, true))),
        ("salary INT", Seq(StructField("salary", IntegerType, true))),
        ("salary int", Seq(StructField("salary", IntegerType, true))),
        ("name STRING NOT NULL", Seq(StructField("name", StringType, false))),
        ("name STRING, salary INT", Seq(StructField("name", StringType, true), StructField("salary", IntegerType, true))),
        ("name STRING NOT NULL, salary INT NOT NULL", Seq(StructField("name", StringType, false), StructField("salary", IntegerType, false))),
      )
    forAll(scenarios) { (inp, res) =>
      parseSchema(inp) should be (Some(StructType(res)))
    }
  }

  test("parse failure") {
    val scenarios =
      Table(
        "input",
        "",
        "name",
        "name FOO",
        "name STRING NULL",
        "name STRING NOT NULL PRIMARY KEY",
      )
    forAll(scenarios) { inp =>
      parseSchema(inp) should be (None)
    }
  }

}
