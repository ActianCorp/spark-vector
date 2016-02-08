/*
 * Copyright 2016 Actian Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.actian.spark_vectorh.vector

import org.scalatest.{FunSuite, Matchers}
import org.scalatest.prop.PropertyChecks

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
    forAll(invalidCombos) { (host: String, instance: String, database: String) =>
      a[IllegalArgumentException] should be thrownBy {
        VectorConnectionProperties(host, instance, database)
      }
    }
  }

  private def validate(props: VectorConnectionProperties, host: String, instance: String, database: String, user: Option[String], password: Option[String], expectedURL: String): Unit = {
    props.toJdbcUrl should be(expectedURL)
    props.host should be(host)
    props.instance should be(instance)
    props.database should be(database)
    props.user should be(user)
    props.password should be(password)
  }
}
