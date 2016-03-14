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
package com.actian.spark_vector.colbuffer

import com.actian.spark_vector.colbuffer.integer._
import com.actian.spark_vector.colbuffer.real._
import com.actian.spark_vector.colbuffer.decimal._
import com.actian.spark_vector.colbuffer.singles._
import com.actian.spark_vector.colbuffer.string._
import com.actian.spark_vector.colbuffer.time._
import com.actian.spark_vector.colbuffer.timestamp._

import java.nio.ByteBuffer
import java.nio.ByteOrder

import scala.reflect.{ClassTag, classTag}
import scala.throws._

/**
 * Abstract class to be used when implementing the class for a typed ColumnBuffer
 * (e.g. object IntColumnBuffer extends ColumnBuffer[Int])
 *
 * This class implements the base methods for buffering vectors of column values.
 * The `put`, `flip`, `clear` methods are according to the Buffer interface.
 * The column value serialization should be implemented within the concrete
 * (typed) class instead.
 *
 * @name the column's name
 * @maxValueCount the maximum number of values to store within the buffer
 * @valueWidth the width of the value's data type
 * @alignSize the data type's alignment size
 * @nullable whether this column accepts null values or not
 */
abstract class ColumnBuffer[@specialized T: ClassTag](name: String, maxValueCount: Int, valueWidth: Int, val alignSize: Int, val nullable: Boolean) {
  private final val NullMarker = 1:Byte
  private final val NonNullMarker = 0:Byte

  val valueType = classTag[T]
  val values = ByteBuffer.allocateDirect(maxValueCount * valueWidth).order(ByteOrder.nativeOrder())
  val markers = ByteBuffer.allocateDirect(maxValueCount).order(ByteOrder.nativeOrder())
  private val nullValue = Array.fill[Byte](alignSize)(0:Byte)

  protected def put(source: T, buffer: ByteBuffer): Unit

  def put(source: T): Unit = {
    put(source, values)
    if (nullable) {
      markers.put(NonNullMarker)
    }
  }

  @throws(classOf[IllegalArgumentException])
  def putNull(): Unit = {
    if (!nullable) {
      throw new IllegalArgumentException(s"Cannot store NULL values in non-nullable '${name}' column.")
    }
    markers.put(NullMarker)
    values.put(nullValue)
  }

  def size: Int = {
    var ret = values.position()
    if (nullable) {
      ret += markers.position()
    }
    ret
  }

  def flip(): Unit = {
    values.flip()
    if (nullable) {
      markers.flip()
    }
  }

  def clear(): Unit = {
    values.clear()
    if (nullable) {
      markers.clear()
    }
  }
}

/**
 * Trait to be used when implementing a companion object for a typed ColumnBuffer
 * (e.g. object IntColumnBuffer extends ColumnBufferInstance[Int])
 */
private[colbuffer] trait ColumnBufferBuilder {
  protected def isInBounds(value: Int, bounds: (Int, Int)): Boolean = (bounds._1 <= value && value <= bounds._2)

  /** Get a new instance of `ColumnBuffer` for the given column type params. */
  private[colbuffer] val build: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_]]
}

/**
 * Case class to be used when trying to create a typed column buffer object
 * using the `ColumnBuffer(..)` apply-factory call with data type specific params.
 *
 * @name the column's name
 * @tpe the data type's name (required in lower cases)
 * @precision the data type's precision
 * @scale the data type's scale size
 * @maxValueCount the size of this column buffer (in tuple/value counts)
 * @nullable whether this column accepts null values or not
 */
case class ColumnBufferBuildParams(name: String, tpe: String, precision: Int, scale: Int, maxValueCount: Int, nullable: Boolean) {
  require(tpe == tpe.toLowerCase, s"Column type '${tpe}' should be in lower case letters.")
}

/** This is a `Factory` implementation of `ColumnBuffers`. */
object ColumnBuffer {
  private final val colBufBuilders:List[ColumnBufferBuilder] = List(
    ByteColumnBuffer,
    ShortColumnBuffer,
    IntColumnBuffer,
    LongColumnBuffer,
    FloatColumnBuffer,
    DoubleColumnBuffer,
    DecimalColumnBuffer,
    BooleanColumnBuffer,
    DateColumnBuffer,
    ByteEncodedStringColumnBuffer,
    IntegerEncodedStringColumnBuffer,
    TimeColumnBuffer,
    TimestampColumnBuffer
  )

  private val build = colBufBuilders.map(_.build).reduce(_ orElse _)

  /**
   * Get the `ColumnBuffer` object for the given `ColumnBufferBuildParams` params.
   * @return an Option embedding the `ColumnBuffer` object (or an empty option if a `ColumnBuffer` was not found)
   */
  def apply(p: ColumnBufferBuildParams): Option[ColumnBuffer[_]] = PartialFunction.condOpt(p)(build)
}
