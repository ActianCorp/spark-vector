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
package com.actian.spark_vectorh

import org.apache.spark.sql.types._
import org.scalacheck._

object DataTypeGens {

  import org.scalacheck.Arbitrary._
  import org.scalacheck.Gen._

  val DefaultMaxNumFields = 10

  private val decimalTypeGen: Gen[DecimalType] =
    for {
      precision <- choose(1, 32)
      scale <- choose(0, precision - 1)
    } yield DecimalType(precision, scale)

  val dataTypeGen: Gen[DataType] =
    oneOf(
      const(BooleanType),
      const(ByteType),
      const(ShortType),
      const(IntegerType),
      const(LongType),
      const(FloatType),
      const(DoubleType),
      const(DateType),
      const(TimestampType),
      const(StringType))

  val fieldGen: Gen[StructField] =
    for {
      name <- identifier
      dataType <- dataTypeGen
      nullable <- arbitrary[Boolean]
    } yield StructField(name, dataType, nullable)

  // TODO ugly dance to avoid duplicate field names - find cleaner way
  val schemaGen: Gen[StructType] =
    for {
      initialNumFields <- choose(1, DefaultMaxNumFields)
      fieldNames <- listOfN(initialNumFields, identifier)
      uniqueFieldNames = fieldNames.distinct
      numFields = uniqueFieldNames.size
      fieldTypes <- listOfN(numFields, dataTypeGen)
      nullables <- listOfN(numFields, arbitrary[Boolean])
    } yield {
      val fields =
        for (idx <- 0 until numFields)
          yield StructField(uniqueFieldNames(idx), fieldTypes(idx), nullables(idx))
      StructType(fields)
    }
}
