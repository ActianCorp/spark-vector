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

import java.util.regex.Pattern

import org.apache.spark.sql.types.DecimalType

import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalacheck.Gen.{choose, identifier}
import org.scalacheck.Prop.{forAll, propBoolean}
import org.scalatest.{FunSuite, Matchers}

import com.actian.spark_vector.test.tags.RandomizedTest

class ColumnMetadataTest extends FunSuite with Matchers {
  // Generate random column metadata and ensure the resultant StructField's are valid
  test("generated", RandomizedTest) {
    forAll(columnMetadataGen)(colMD => {
      assertColumnMetadata(colMD)
    }).check
  }

  val milliSecsPattern = Pattern.compile(".*\\.(S*)")

  def assertColumnMetadata(columnMD: ColumnMetadata): Boolean = {
    val structField = columnMD.structField
    structField.dataType match {
      // For decimal type, ensure the scale and precision match
      case decType: DecimalType =>
        decType.precision should be(columnMD.precision)
        decType.scale should be(columnMD.scale)
      case _ =>
    }
    true
  }

  val columnMetadataGen: Gen[ColumnMetadata] =
    for {
      name <- identifier
      typeName <- VectorTypeGen.vectorJdbcTypeGen
      nullable <- arbitrary[Boolean]
      precision <- choose(0, 20)
      scale <- choose(0, Math.min(20, precision))
    } yield ColumnMetadata(name, typeName, nullable, precision, scale)
}
