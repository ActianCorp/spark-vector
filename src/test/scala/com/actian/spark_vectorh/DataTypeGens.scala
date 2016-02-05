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
