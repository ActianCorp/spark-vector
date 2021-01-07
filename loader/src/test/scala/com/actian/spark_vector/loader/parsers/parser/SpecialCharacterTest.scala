package com.actian.spark_vector.loader.parsers

import org.scalatest._
import org.scalatest.funsuite.FixtureAnyFunSuite
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.actian.spark_vector.loader.command.CSVRead
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import com.actian.spark_vector.loader.options.UserOptions
import resource._
import collection.JavaConverters._

class SpecialCharacterTest extends FixtureAnyFunSuite {

    override type FixtureParam = SparkSession

    override protected def withFixture(test: OneArgTest): Outcome = {
        val conf = new SparkConf()
        .setMaster("local[1]")
        .setAppName("special character test")
        managed(SparkSession.builder.config(conf).getOrCreate()).acquireAndGet { spark =>
        withFixture(test.toNoArgTest(spark))
        }
    }

    test("backslash as escape in CSV files"){ implicit spark =>
        val parser: scopt.OptionParser[UserOptions] = Parser
        val expectedData = List((1,"\"ten\""), (2,"\\N"))
        val schema = StructType( Seq(
            StructField(name = "c1", dataType = IntegerType, nullable = false),
            StructField(name = "c2", dataType = StringType, nullable = false)))
        val loadCommand: Array[String] = Array("load", "csv", "-sf", "../testdata/escaping.csv",
            "-vh", "localhost", "-vi", "VW", "-vd", "testdb", "-tt", "sl_m3812", "-pm", "PERMISSIVE",
            "-sc", ",", "-is", "true", "-qc", "\"", "-ec", "\\", "-h","c1 int, c2 string,")

        val userOptions = parser.parse(loadCommand, UserOptions())
        assert(userOptions != None)

        val expectedRDD = spark.sparkContext.parallelize(expectedData)
        val expectedDF = spark.createDataFrame(expectedData.map({case (x,y) => Row(x,y)}).asJava, schema)
        assert(expectedDF.count() == 2)

        val selectQuery = CSVRead.registerTempTable(userOptions.get, spark.sqlContext)
        val parsedDF = spark.sql(selectQuery)
        assert(parsedDF.count() == 2)

        assert(parsedDF.except(expectedDF).count() == 0)
    }
}
