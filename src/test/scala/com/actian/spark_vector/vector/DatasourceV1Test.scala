package com.actian.spark_vector.vector

import com.actian.spark_vector.SparkContextFixture
import com.actian.spark_vector.sql.{TableRef, VectorRelation}
import com.actian.spark_vector.vector.VectorFixture.withTable
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest._
import org.scalatest.funsuite.FixtureAnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.JavaConverters.seqAsJavaList

import matchers.should._

/** This class contains tests of our Vector Spark DatasourceV1 implementation.
  */
class DatasourceV1Test
    extends FixtureAnyFunSuite
    with Matchers
    with ScalaCheckPropertyChecks
    with SparkContextFixture
    with VectorFixture {

  test("II-7602 - Source and target have different schema column ordering") {
    fixture =>
      val schema = new StructType()
        .add(StructField("col1", IntegerType, true))
        .add(StructField("col2", StringType, true))
        .add(StructField("col3", IntegerType, true))
      val data = Seq(Row(42, "Hello", 1), Row(43, "World", 2))
      val df = fixture.spark.createDataFrame(seqAsJavaList(data), schema)
      def createTable(tableName: String): Unit = {
        VectorJDBC.withJDBC(connectionProps) { cxn =>
          cxn.dropTable(tableName)
          cxn.executeStatement(s"""|
              |  create table ${tableName} (
              |  col2 VARCHAR(20),
              |  col1 int,
              |  col3 int
              |) WITH NOPARTITION""".stripMargin)
        }
      }

      withTable(createTable) { tableName =>
        val tableRef = TableRef(connectionProps, tableName)
        val vecRel = VectorRelation(
          tableRef,
          fixture.spark.sqlContext,
          Map.empty[String, String]
        )
        vecRel.insert(df, false)
        VectorJDBC.withJDBC(connectionProps) { cxn =>
          val res = cxn.query(s"""select col1, col2, col3 from ${tableName}""")
          res.sortBy(x => x(0).asInstanceOf[Int]) shouldBe data.map(_.toSeq)
        }
      }
  }
}
