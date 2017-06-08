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

import java.math.BigDecimal
import java.{ sql => jsql }
import java.util.Calendar

import scala.collection.Seq
import scala.util.Try

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalacheck.Gen

import com.actian.spark_vector.colbuffer.util.MillisecondsInDay
import java.math.RoundingMode

object DataGens {
  import com.actian.spark_vector.DataTypeGens._
  import org.scalacheck.Arbitrary._
  import org.scalacheck.Gen._
  import scala.collection.JavaConverters._

  val DefaultMaxRows = 2049

  val booleanGen: Gen[Boolean] = arbitrary[Boolean]

  val byteGen: Gen[Byte] = arbitrary[Byte]

  val shortGen: Gen[Short] = arbitrary[Short]

  val intGen: Gen[Int] = arbitrary[Int]

  val longGen: Gen[Long] = arbitrary[Long]

  // FIXME allow arbitrary doubles (and filter externally for vector tests)
  val floatGen: Gen[Float] = arbitrary[Float].map(f => if (f.abs > 1e-38) f else 0.0f)

  // FIXME allow arbitrary doubles (and filter externally for vector tests)
  val doubleGen: Gen[Double] = for {
    neg <- arbitrary[Boolean]
    digits <- listOfN(12, choose(0, 9))
  } yield s"${if (neg) "-" else ""}1.${digits.mkString("")}".toDouble

  val decimalGen: Gen[BigDecimal] = arbitrary[scala.BigDecimal].filter(bd =>
    bd.scale <= 12 && bd.precision <= 38 && Try { new BigDecimal(bd.toString) }.isSuccess).map(bd => new BigDecimal(bd.toString))

  private val dateValueGen: Gen[Long] =
    choose(-3600L * 1000 * 24 * 100000L, 3600L * 1000 * 24 * 100000L)

  // @note normalize getTime so that we don't have diffs more than 1 day in between our {JDBC,Spark}results
  val dateGen: Gen[jsql.Date] = dateValueGen.map(d => new jsql.Date(d / MillisecondsInDay * MillisecondsInDay))

  val timestampGen: Gen[jsql.Timestamp] = for (ms <- dateValueGen) yield new jsql.Timestamp(ms)

  // FIXME allow empty strings (and filter externally for vector tests)
  // @note we donnot allow invalid UTF8 chars to be generated (from D800 to DFFF incl)
  val stringGen: Gen[String] =
    arbitrary[String].map(s => if (s != null) s.filter(c => Character.isDefined(c) && c != '\u0000' && (c < '\uD800' || c > '\uDFFF')) else s).filter(s => s == null || !s.isEmpty)

  def valueGen(dataType: DataType): Gen[Any] = dataType match {
    case BooleanType => booleanGen
    case ByteType => byteGen
    case ShortType => shortGen
    case IntegerType => intGen
    case LongType => longGen
    case FloatType => floatGen
    case DoubleType => doubleGen
    case TimestampType => timestampGen
    case DateType => dateGen
    case StringType => stringGen
    case _: DecimalType => decimalGen
    case _ => throw new Exception("Invalid data type.")
  }

  def nullableValueGen(field: StructField): Gen[Any] = {
    val gen = valueGen(field.dataType)
    if (field.nullable) frequency(1 -> gen, 10 -> const(null)) else gen
  }

  def rowGen(schema: StructType): Gen[Row] =
    sequence(schema.fields.map(f => nullableValueGen(f))).map(l => Row.fromSeq(l.asScala)) // TODO Huh? Why ju.ArrayList?!?

  def dataGenFor(schema: StructType, maxRows: Int): Gen[Seq[Row]] = for {
    numRows <- choose(1, maxRows)
    rows <- listOfN(numRows, rowGen(schema))
  } yield rows

  case class TypedData(dataType: StructType, data: Seq[Row])

  val dataGen: Gen[TypedData] = for {
    schema <- schemaGen
    data <- dataGenFor(schema, DefaultMaxRows)
  } yield TypedData(schema, data)
}
