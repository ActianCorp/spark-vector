package com.actian.spark_vectorh.vector

import java.sql.SQLNonTransientConnectionException

import org.scalatest.{ BeforeAndAfter, FunSuite, Matchers }

import com.actian.spark_vectorh.test.IntegrationTest
import com.actian.spark_vectorh.vector.ErrorCodes._
import com.actian.spark_vectorh.vector.VectorFixture._
import com.actian.spark_vectorh.vector.VectorJDBC._

/**
 * Tests of VectorJDBC
 */
@IntegrationTest
class VectorJDBCTest extends FunSuite with BeforeAndAfter with Matchers with VectorFixture {

  private val doesNotExistTable = "this_table_does_not_exist"
  private val typeTable = "test_types"
  private val testTable = "test_vector"

  before {
    VectorJDBC.withJDBC(connectionProps) { cxn =>
      cxn.dropTable(doesNotExistTable)
      cxn.dropTable(typeTable)
      cxn.executeStatement(createTableStatement(typeTable, allTypesColumnMD))
      cxn.dropTable(testTable)
      cxn.executeStatement(s"create table $testTable (col1 integer not null)")
    }
  }

  test("tableExists for non-existant table") {
    withJDBC(connectionProps) {cxn =>
      val exists = cxn.tableExists(doesNotExistTable)
      exists should be (false)
    }
  }

  test("columnMetadata for non-existant table") {
    withJDBC(connectionProps) { cxn =>
      intercept[Exception] {
        cxn.columnMetadata(doesNotExistTable)
      }
    }
  }

  test("tableExists for existing table") {
    withJDBC(connectionProps) { cxn =>
      val exists = cxn.tableExists(typeTable)
      exists should be (true)
    }
  }

  test("get column metadata for an existing table") {
    withJDBC(connectionProps) { cxn =>
        cxn.columnMetadata(typeTable) should be (allTypesColumnMD)
    }
  }

  test("bad connection") {
    val badCxnProps = VectorConnectionProperties("host", "instance", "database", Some("user"), Some("pw"))
    intercept[SQLNonTransientConnectionException] {
      withJDBC(badCxnProps) { cxn =>
        assert(false, "should not get here")
      }
    }
  }

  test("empty SQL statements") {
    executeStatements(connectionProps)(Seq[String]())
  }

  test("single SQL statements") {
    executeStatements(connectionProps)(Seq(s"insert into $testTable values (1)"))

    VectorJDBC.withJDBC(connectionProps) { cxn =>
      val rowCount = cxn.querySingleResult(s"select count(*) from $testTable")
      rowCount should be (Some(1))
    }
  }

  test("multiple SQL statements") {
    val statements = Seq(
      s"insert into $testTable values (1)",
      s"insert into $testTable values (2)"
    )
    executeStatements(connectionProps)(statements)

    VectorJDBC.withJDBC(connectionProps) { cxn =>
      val rowCount = cxn.querySingleResult("select count(*) from " + testTable)
      rowCount should be (Some(2))
    }
  }

  test("Mix of statements that work and fail") {

    val statements = Seq(
      s"insert into $testTable values (1)", // this works
      s"insert into $testTable values ('hello there')" // this doesn't
    )

    val ex = intercept[VectorException] {
      executeStatements(connectionProps)(statements)
    }

    ex should not be null
    ex.errorCode should be(SqlExecutionError)

    // Ensure no data was committed since once query failed
    VectorJDBC.withJDBC(connectionProps) { cxn =>
      val rowCount = cxn.querySingleResult("select count(*) from " + testTable)
      rowCount should be(Some(0))
    }
  }
}

