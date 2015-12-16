package com.actian.spark_vectorh.vector

import org.scalatest.prop.PropertyChecks
import org.scalatest.{FunSuite, Matchers}


class VectorConnectionPropertiesTest extends FunSuite with Matchers with PropertyChecks {
  val validCombos = Table(
    ("host", "instance", "database", "user", "password", "expectedURL"),
    ("host.com", "VH", "db", Some("user"), Some("pw"), "jdbc:ingres://host.com:VH7/db"),
    ("some.host.com", "VW", "db", None, None, "jdbc:ingres://some.host.com:VW7/db"),
    ("justhost", "99", "mydatabase", None, None, "jdbc:ingres://justhost:997/mydatabase"))

  val invalidCombos = Table(
    ("host", "instance", "database"),
    (null, "VW", "database"),
    ("", "VW", "database"),
    ("host.com", null, "db"),
    ("host.com", "", "db"),
    ("host.com", "VW", null),
    ("host.com", "VW", ""))

  test("valid URL and values") {
    forAll(validCombos) { (host: String, instance: String, database: String, user: Option[String], password: Option[String], expectedURL: String) =>
      // With user & password
      val props = VectorConnectionProperties(host, instance, database, user, password)
      validate(props, host, instance, database, user, password, expectedURL)

      // Without user & password
      val props2 = VectorConnectionProperties(host, instance, database)
      validate(props2, host, instance, database, None, None, expectedURL)
    }
  }

  test("invalid vector properties") {
    forAll(invalidCombos) {(host: String, instance: String, database: String) =>
      a [IllegalArgumentException] should be thrownBy {
        VectorConnectionProperties(host, instance, database)
      }
    }
  }

  private def validate(props: VectorConnectionProperties, host: String, instance: String, database: String, user: Option[String], password: Option[String], expectedURL: String ): Unit = {
    props.toJdbcUrl should be (expectedURL)
    props.host should be (host)
    props.instance should be (instance)
    props.database should be (database)
    props.user should be (user)
    props.password should be (password)
  }
}
