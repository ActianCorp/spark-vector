package com.actian.spark_vector.external_table

import com.actian.spark_vector.test.IntegrationTest
import com.actian.spark_vector.vector.VectorJDBC
import org.scalatest._
import org.scalatest.funsuite.FixtureAnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import matchers.should._

@IntegrationTest
/** This class represents a full Vector(H)-Spark-Provider integration scenario
  * testing the staging table feature.
  */
class StagingTableTest
    extends FixtureAnyFunSuite
    with Matchers
    with ScalaCheckPropertyChecks
    with LocalProviderFixture {

  test("II-7604 - Staging with string replace") { f =>
    val values: Seq[Seq[Any]] = Seq(Seq(1, "test"))
    val expected: Seq[Seq[Any]] = Seq(Seq(1, "tist"))
    val tableName = s"staging_test"
    val extTableName = tableName + "_ext"
    val createTable =
      s"CREATE TABLE ${tableName} (id Int, text VARCHAR(20)) with NOPARTITION"
    val insertStatement =
      s"""INSERT INTO ${tableName} values(${values(0)(0)}, '${values(0)(1)}')"""
    val createExternalTable =
      s"""|CREATE EXTERNAL TABLE ${extTableName}(id Int, text VARCHAR(20)) USING SPARK WITH REFERENCE='dummy',
			|FORMAT='vector',
			|OPTIONS=('host' = '${connectionProps.host}',
      			|'port'='${connectionProps.port.toString()}',
    			|'database'='${connectionProps.database}',
				|'table' = '${tableName}',
    			|'user'='${connectionProps.user.getOrElse("")}',
    			|'password'='${connectionProps.password.getOrElse("")}',
				|'staging' = 'select id, replace(text, "e", "i") as text from THIS_TABLE')""".stripMargin
    val select = s"""select * from ${extTableName}"""

    noException shouldBe thrownBy(VectorJDBC.withJDBC(connectionProps)(cxn => {
      cxn.dropTable(tableName)
      cxn.executeStatement(createTable)
      cxn.executeStatement(insertStatement)
    }))

    noException shouldBe thrownBy(VectorJDBC.withJDBC(connectionProps)(cxn => {
      cxn.dropTable(extTableName)
      cxn.executeStatement(createExternalTable)
    }))

    VectorJDBC.withJDBC(connectionProps)(cxn => {
      val res = cxn.query(select)
      res.size shouldBe 1
      res(0).size shouldBe 2
      res(0)(0) shouldBe expected(0)(0)
      res(0)(1) shouldBe expected(0)(1)
    })

    noException shouldBe thrownBy(VectorJDBC.withJDBC(connectionProps)(cxn => {
      cxn.dropTable(tableName)
      cxn.dropTable(extTableName)
    }))
  }
}
