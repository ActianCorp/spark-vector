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
package com.actian.spark_vector.vector

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should._

import org.scalatest.prop.TableDrivenPropertyChecks

class VectorConnectionPropertiesTest extends AnyFunSuite with Matchers with TableDrivenPropertyChecks {
  val validCombos = Table(
    ("host",          "instance", "instanceOffset", "port",       "database",   "user",      "password",  "expectedURL"),
    ("host.com",      Some("VH"), None,             None,         "db",         Some("user"), Some("pw"), "jdbc:ingres://host.com:VH7/db"),
	("host.com",      Some("VH"), Some("8"),        None,         "db",         Some("user"), Some("pw"), "jdbc:ingres://host.com:VH8/db"),
	("host.com",      Some("A1"), Some("8"),        None,         "db",         Some("user"), Some("pw"), "jdbc:ingres://host.com:A18/db"),
    ("some.host.com", None,       None,             Some("9000"), "db",         None,         None,       "jdbc:ingres://some.host.com:9000/db"),
	("justhost",      None,       None,             Some("VW8"),  "mydatabase", None,         None,       "jdbc:ingres://justhost:VW8/mydatabase"),
	("justhost",      None,       None,             Some("A18"),  "mydatabase", None,         None,       "jdbc:ingres://justhost:A18/mydatabase"))

  val invalidCombos = Table(
    (null, "database"),
    ("", "database"),
    ("host.com", null),
    ("host.com", ""))

  val invalidCombos2 = Table(
    ("instance", "instanceOffset", "port"  ),
    (None,       None,             None    ),
    (Some(""),   None,             None    ),
    (Some("VW"), None,             Some("7")),
    (Some("VW"), Some("7"),        Some("7")),
    (Some("VW"), Some(""),         None    ),
    (Some("VW"), Some(""),         Some("")),
    (Some("VW"), None,             Some("")),
    (None,       None,             Some("")),
    (None,       Some("7"),        Some("9")),
    (Some("8"),  None,             None    ),
    (Some("VW"), Some("A"),        None    ),
	(None,       None,             Some("VW")),
	(None,       None,             Some("W1")))

  test("valid URL and values") {
    forAll(validCombos) { (host: String, instance: Option[String], instanceOffset: Option[String], port: Option[String], database: String, user: Option[String], password: Option[String], expectedURL: String) =>
        var jdbcPort: Option[JDBCPort] = None
        noException shouldBe thrownBy {
            jdbcPort = Some(JDBCPort(instance, instanceOffset, port))
        }
        // With user & password
          val props = VectorConnectionProperties(host, jdbcPort.get, database, user, password)
          validate(props, host, jdbcPort.get, database, user, password, expectedURL)

          // Without user & password
          val props2 = VectorConnectionProperties(host, jdbcPort.get, database)
          validate(props2, host, jdbcPort.get, database, None, None, expectedURL)
    }
  }

  test("invalid host or database") {
    forAll(invalidCombos) { (host: String, database: String) =>
      a[IllegalArgumentException] should be thrownBy {
        VectorConnectionProperties(host, JDBCPort(None, None, Some("9999")), database)
      }
    }
  }

  test("invalid instance or instanceOffset or port") {
    forAll(invalidCombos2) { (instance: Option[String], instanceOffset: Option[String], port: Option[String]) =>
        a[IllegalArgumentException] should be thrownBy {
            val jdbcPort = JDBCPort(instance, instanceOffset, port)
        }
    }
  }

  private def validate(props: VectorConnectionProperties, host: String, port: JDBCPort, database: String, user: Option[String], password: Option[String], expectedURL: String): Unit = {
    props.toJdbcUrl should be(expectedURL)
    props.host should be(host)
    props.port.value should be(port.value)
    props.database should be(database)
    props.user should be(user)
    props.password should be(password)
  }
}
