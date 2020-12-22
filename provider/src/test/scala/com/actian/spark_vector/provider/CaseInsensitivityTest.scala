package com.actian.spark_vector.provider

import org.scalatest._
import org.scalatest.prop._
import org.apache.spark.sql.SparkSession
import resource._
import org.apache.spark.SparkConf

class CaseInsensivitiyTest extends fixture.FunSuite with Matchers with PrivateMethodTester with TableDrivenPropertyChecks
{
    override type FixtureParam = SparkSession

    override protected def withFixture(test: OneArgTest): Outcome = {
        val conf = new SparkConf()
        .setMaster("local[1]")
        .setAppName("case sensitivity test")
        managed(SparkSession.builder.config(conf).getOrCreate()).acquireAndGet { spark =>
        withFixture(test.toNoArgTest(spark))
        }
    }

    test("Job options case insensitive")
    {
        implicit spark =>
        val scenarios =
        Table(
            ("input", "result"),
            (Map("header"-> "true"), Map("header"-> "true")),
            (Map("HEADER"-> "true"), Map("header"-> "true")),
            (Map("Header"-> "true"), Map("header"-> "true"))
        )
        val getOptions = PrivateMethod[Map[String, String]]('getOptions)
        val handler = new RequestHandler(spark , ProviderAuth("test", "test"))
        forAll(scenarios) { (inp, res) =>
            val jobPart = new JobPart("1", "test", "test op", "path", Some("csv"), Seq(new ColumnInfo("test", new LogicalType("Int", 4, 4), "Int", false, None)), Some(inp), new DataStream("test", "test", Seq(new StreamPerNode(1, 1, "test"))))
            handler invokePrivate getOptions(jobPart) should be (res)
        }
    }

    test("Job extra options case insensitive")
    {
        implicit spark =>
        val scenarios =
        Table(
            ("input", "result"),
            (Map("schema"-> "test", "filter"-> "test"), Map("schema"-> "test", "filter"-> "test")),
            (Map("SCHEMA"-> "test", "FILTER"-> "test"), Map("schema"-> "test", "filter"-> "test")),
            (Map("Schema"-> "test", "filtER"-> "test"), Map("schema"-> "test", "filter"-> "test"))
        )
        val getExtraOptions = PrivateMethod[Map[String, String]]('getExtraOptions)
        val handler = new RequestHandler(spark , ProviderAuth("test", "test"))
        forAll(scenarios) { (inp, res) =>
            val jobPart = new JobPart("1", "test", "test op", "path", Some("csv"), Seq(new ColumnInfo("test", new LogicalType("Int", 4, 4), "Int", false, None)), Some(inp), new DataStream("test", "test", Seq(new StreamPerNode(1, 1, "test"))))
            handler invokePrivate getExtraOptions(jobPart) should be (res)
        }
    }
}