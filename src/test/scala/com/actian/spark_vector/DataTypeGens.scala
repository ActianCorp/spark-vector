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
package com.actian.spark_vector

import org.apache.spark.sql.types._
import org.scalacheck._

object DataTypeGens {
  import org.scalacheck.Arbitrary._
  import org.scalacheck.Gen._

  val DefaultMaxNumFields = 10
  val MaxColumnNameLen = 30

  // FIXME DecimalType doesn't exist yet in Spark 1.5.2
  private val decimalTypeGen: Gen[DecimalType] = for {
    precision <- choose(1, 32)
    scale <- choose(0, precision - 1)
  } yield DecimalType(precision, scale)

  val dataTypeList: List[DataType] = List(
      BooleanType,
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      DateType,
      TimestampType,
      StringType,
      DecimalType(38, 12)
      )
      
  val dataTypeGen: Gen[DataType] = oneOf(dataTypeList)

  val fieldGen: Gen[StructField] = for {
    name <- identifier
    dataType <- dataTypeGen
    nullable <- arbitrary[Boolean]
  } yield StructField(name, dataType, nullable)

  val schemaGen: Gen[StructType] = for {
    numFields <- choose(1, DefaultMaxNumFields)
    uniqueFieldNames <- listOfN(numFields, identifier.map(_.take(MaxColumnNameLen))).retryUntil(f => f.size == f.distinct.size)
    fieldTypes <- listOfN(numFields, dataTypeGen)
    nullables <- listOfN(numFields, arbitrary[Boolean])
  } yield StructType(for (idx <- 0 until numFields) yield StructField(
    uniqueFieldNames(idx), fieldTypes(idx), nullables(idx)))
  
  val allTypesSchemaGen: Gen[StructType] = for {
    numfields <- dataTypeList.size
    uniqueFieldNames <- listOfN(numfields, identifier.map(_.take(MaxColumnNameLen))).retryUntil(f => f.size == f.distinct.size)
    fieldTypes <- pick(numfields, dataTypeList)
  } yield StructType(for (idx <- 0 until numfields) yield StructField(
    uniqueFieldNames(idx), fieldTypes(idx), false))
}
