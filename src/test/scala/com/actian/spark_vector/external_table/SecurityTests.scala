package com.actian.spark_vector.external_table

import com.actian.spark_vector.test.IntegrationTest
import com.actian.spark_vector.vector.VectorJDBC
import org.scalatest._
import org.scalatest.{ FunSuite, Matchers }
import org.scalatest.prop.PropertyChecks

@IntegrationTest
/** This class represents a full Vector(H)-Spark-Provider integration scenario
  * testing security features.
  */
class SecurityTests
    extends fixture.FunSuite
    with Matchers
    with PropertyChecks
    with LocalProviderFixture {


  test("II-7782 - JDBC connections require user and password") { f =>
    val values: Seq[Seq[Any]] = Seq(Seq(1, "test"))
    val tableName = s"jdbc_test"
    val extTableName = tableName + "_ext"

    val createTable =
      s"CREATE TABLE ${tableName} (id Int, text VARCHAR(20)) with NOPARTITION"
    val insertStatement =
      s"""INSERT INTO ${tableName} values(${values(0)(0)}, '${values(0)(1)}')"""
    val select = s"""select * from """

    val usersJDBCWorking = Seq(
      s"""user=${connectionProps.user.getOrElse("")}""",
      s"""UID=${connectionProps.user.getOrElse("")}"""
    )
    val pwJDBCWorking = Seq(
      s"""password=${connectionProps.password.getOrElse("")}""",
      s"""PWD=${connectionProps.password.getOrElse("")}"""
    )
    val usersJDBCNotWorking = Seq("", "user=", "UID=")
    val pwJDBCNotWorking = Seq("", "password=", "PWD=")

    val usersOptionsWorking =
      Seq(s"""'user'='${connectionProps.user.getOrElse("")}'""")
    val pwOptionsWorking =
      Seq(s"""'password'='${connectionProps.password.getOrElse("")}'""")
    val usersOptionsNotWorking = Seq("", s"""'user'=''""")
    val pwOptionsNotWorking = Seq("", s"""'password'=''""")

    noException shouldBe thrownBy(VectorJDBC.withJDBC(connectionProps)(cxn => {
      cxn.dropTable(tableName)
      cxn.executeStatement(createTable)
      cxn.executeStatement(insertStatement)
    }))

    val workingCandidates = (for {
      i <- usersJDBCWorking
      j <- pwJDBCWorking
    } yield (i, j, "", "")) ++ (for {
      i <- usersOptionsWorking
      j <- pwOptionsWorking
    } yield ("", "", i, j))

    val workingScenarios =
      Table(("uJDBC", "pJDBC", "uOPTIONS", "pOPTIONS"), workingCandidates: _*)

    val notWorkingCandidates = (for {
      i <- usersJDBCNotWorking
      j <- pwJDBCNotWorking
    } yield (i, j, "", "")) ++ (for {
      i <- usersOptionsNotWorking
      j <- pwOptionsNotWorking
    } yield ("", "", i, j)) ++ (for {
      i <- usersJDBCWorking
      j <- pwJDBCWorking
      k <- usersOptionsWorking
      l <- pwOptionsWorking
    } yield (i, j, k, l))

    val notWorkingScenarios = Table(
      ("uJDBC", "pJDBC", "uOPTIONS", "pOPTIONS"),
      notWorkingCandidates: _*
    )

    forAll(workingScenarios) { (i, j, k, l) =>
      {
        noException shouldBe thrownBy(
          VectorJDBC.withJDBC(connectionProps)(cxn => {
            cxn.dropTable(extTableName + "_1")
            cxn.dropTable(extTableName + "_2")
          })
        )

        val createExternalTable_1 =
          s"""|CREATE EXTERNAL TABLE ${extTableName}_1 (id Int, text VARCHAR(20)) USING SPARK WITH REFERENCE='dummy',
            |FORMAT='jdbc',
            |OPTIONS=('url' = 'jdbc:ingres://${connectionProps.host}:${connectionProps.port
            .toString()}/${connectionProps.database}${if (i.isEmpty()) ""
          else ";"}${i}${if (j.isEmpty()) "" else ";"}${j}',
            |'driver' = 'com.ingres.jdbc.IngresDriver',
            |'dbtable' = '${tableName}'${if (k.isEmpty()) "" else ","} ${k}${if (
            l.isEmpty()
          ) ""
          else ","} ${l})""".stripMargin

        val createExternalTable_2 =
          s"""|CREATE EXTERNAL TABLE ${extTableName}_2 (id Int, text VARCHAR(20)) USING SPARK WITH REFERENCE='dummy',
            |FORMAT='jdbc',
            |OPTIONS=('url' = 'jdbc:ingres://${connectionProps.host}:${connectionProps.port
            .toString()}/${connectionProps.database}${if (j.isEmpty()) ""
          else ";"}${j}${if (i.isEmpty()) "" else ";"}${i}',
            |'driver' = 'com.ingres.jdbc.IngresDriver',
            |'dbtable' = '${tableName}'${if (k.isEmpty()) "" else ","} ${k}${if (
            l.isEmpty()
          ) ""
          else ","} ${l})""".stripMargin

        noException shouldBe thrownBy(
          VectorJDBC.withJDBC(connectionProps)(cxn => {
            cxn.executeStatement(createExternalTable_1)
            cxn.executeStatement(createExternalTable_2)
          })
        )

        VectorJDBC.withJDBC(connectionProps)(cxn => {
          val res = cxn.query(select + s"""${extTableName}_1""")
          val res2 = cxn.query(select + s"""${extTableName}_2""")

          res shouldBe values
          res2 shouldBe values
        })

        noException shouldBe thrownBy(
          VectorJDBC.withJDBC(connectionProps)(cxn => {
            cxn.dropTable(extTableName + "_1")
            cxn.dropTable(extTableName + "_2")
          })
        )
      }
    }

    forAll(notWorkingScenarios) { (i, j, k, l) =>
      {
        noException shouldBe thrownBy(
          VectorJDBC.withJDBC(connectionProps)(cxn => {
            cxn.dropTable(extTableName)
          })
        )

        val createExternalTable =
          s"""|CREATE EXTERNAL TABLE ${extTableName} (id Int, text VARCHAR(20)) USING SPARK WITH REFERENCE='dummy',
            |FORMAT='jdbc',
            |OPTIONS=('url' = 'jdbc:ingres://${connectionProps.host}:${connectionProps.port
            .toString()}/${connectionProps.database}${if (i.isEmpty()) ""
          else ";"}${i}${if (j.isEmpty()) "" else ";"}${j}',
            |'driver' = 'com.ingres.jdbc.IngresDriver',
            |'dbtable' = '${tableName}'${if (k.isEmpty()) "" else ","} ${k}${if (
            l.isEmpty()
          ) ""
          else ","} ${l})""".stripMargin

        noException shouldBe thrownBy(
          VectorJDBC.withJDBC(connectionProps)(cxn => {
            cxn.executeStatement(createExternalTable)
          })
        )

        var thrown =
          intercept[Exception](VectorJDBC.withJDBC(connectionProps)(cxn => {
            cxn.query(select + s"""${extTableName}""")
          }))

        assert(
          thrown
            .getMessage()
            .contains("No user name and/or password provided!") ||
            thrown
              .getMessage()
              .contains(
                "User and password are EITHER allowed in JDBC url OR under OPTIONS!"
              )
        )

        noException shouldBe thrownBy(
          VectorJDBC.withJDBC(connectionProps)(cxn => {
            cxn.dropTable(extTableName)
          })
        )
      }

      noException shouldBe thrownBy(
        VectorJDBC.withJDBC(connectionProps)(cxn => {
          cxn.dropTable(tableName)
        })
      )
    }
  }
}
