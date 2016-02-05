package com.actian.spark_vectorh

import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

import com.actian.spark_vectorh.test.util.StructTypeUtil.createSchema

/**
 * Set of test RDD functions.
 */
trait RDDFixtures {

  // poor man's fixture, for other approaches see:
  // http://www.scalatest.org/user_guide/sharing_fixtures
  def createRecordRdd(sc: SparkContext): (RDD[Seq[Any]], StructType) = {
    val input = Seq(
      Seq(42, "a"),
      Seq(43, "b"))
    val inputRdd = sc.parallelize(input, 2)
    val inputSchema = createSchema("id" -> IntegerType, "name" -> StringType)
    (inputRdd, inputSchema)
  }

  def createRowRDD(sc: SparkContext): (RDD[Seq[Any]], StructType) = {
    val input = Seq(
      Seq[Any](42, "a", new Date(), new Date()),
      Seq[Any](43, "b", new Date(), new Date()))
    val inputRdd = sc.parallelize(input, 2)
    val inputSchema = createSchema("id" -> IntegerType, "name" -> StringType, "date" -> DateType)
    (inputRdd, inputSchema)
  }

  def wideRDD(sc: SparkContext, columnCount: Int, rowCount: Int = 2): (RDD[Seq[Any]], StructType) = {
    val data: Seq[Int] = 1 to columnCount

    val fields = for (i <- 1 to rowCount) yield {
      StructField("field_" + i, IntegerType, true)
    }

    val inputSchema = StructType(fields.toSeq)

    val input = for (i <- 1 to rowCount) yield {
      data.asInstanceOf[Seq[Any]]
    }

    val inputRDD = sc.parallelize(input, 2)
    (inputRDD, inputSchema)
  }
}
