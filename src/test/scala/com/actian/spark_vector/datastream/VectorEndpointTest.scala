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
package com.actian.spark_vector.datastream

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should._
import org.scalatest.PrivateMethodTester
import com.actian.spark_vector.test.IntegrationTest
import com.actian.spark_vector.vector.{VectorJDBC, VectorFixture, ColumnMetadata}

@IntegrationTest
class VectorEndpointTest extends AnyFunSuite with Matchers with VectorFixture with PrivateMethodTester {
    test("iivwtable_datastreams contains host and/or qhost") {
        VectorJDBC.withJDBC(connectionProps) { cxn =>
            noException shouldBe thrownBy{
                VectorEndpoint.fromDataStreamsTable(cxn)
            }
        }
    }

    test("check extractHostColumnName") {
        val extractHostColumnName = PrivateMethod[String]('extractHostColumnName)
        val result_qHost = Seq(ColumnMetadata("qhost", "", false, 0, 0), ColumnMetadata("host", "", false, 0, 0), ColumnMetadata("test", "", false, 0, 0))
        val result_qHost2 = Seq(ColumnMetadata("host", "", false, 0, 0), ColumnMetadata("qhost", "", false, 0, 0))
        val result_qHost3 = Seq(ColumnMetadata("qhost", "", false, 0, 0))
        val result_Host = Seq(ColumnMetadata("host", "", false, 0, 0))
        val result_Exc = Seq(ColumnMetadata("test", "", false, 0, 0))

        VectorEndpoint invokePrivate extractHostColumnName(result_qHost) shouldEqual "qhost"
        VectorEndpoint invokePrivate extractHostColumnName(result_qHost2) shouldEqual "qhost"
        VectorEndpoint invokePrivate extractHostColumnName(result_qHost3) shouldEqual "qhost"
        VectorEndpoint invokePrivate extractHostColumnName(result_Host) shouldEqual "host"

        an[IllegalStateException] shouldBe thrownBy{
            VectorEndpoint invokePrivate extractHostColumnName(result_Exc)
        }
    }
}