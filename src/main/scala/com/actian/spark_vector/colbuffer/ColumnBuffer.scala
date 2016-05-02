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
import com.actian.spark_vector.vector.VectorDataType
import com.actian.spark_vector.datastream.padding

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.BufferOverflowException

import scala.reflect.{ ClassTag, classTag }
import scala.throws._

/**
 * Abstract class to be used when implementing the class for a typed ColumnBuffer
 * (e.g. object IntColumnBuffer extends ColumnBuffer[Int])
 *
 * This class implements the base `put` and `get` methods for serializing (IN-type) and
 * deserializing (OUT-type) vectors of column values.
 *
 * @param name the column's name
 * @param maxValueCount the maximum number of values to store within the buffer
 * @param valueWidth the width of the value's data type
 * @param alignSize the data type's alignment size
 * @param nullable whether this column accepts null values or not
 */
private[colbuffer] abstract class ColumnBuffer[@specialized IN: ClassTag, @specialized OUT: ClassTag](val name: String,
  val maxValueCount: Int, val valueWidth: Int, val alignSize: Int, val nullable: Boolean) extends Serializable {
  val valueTypeIn = classTag[IN]
  val valueTypeOut = classTag[OUT]

  @inline def put(source: IN, buffer: ByteBuffer): Unit

  @inline def get(buffer: ByteBuffer): OUT
}

private[colbuffer] sealed abstract class RWColumnBuffer(col: ColumnBuffer[_, _])
  extends Serializable {
  protected final val NullMarker = 1: Byte
  protected final val NonNullMarker = 0: Byte

  val alignSize = col.alignSize
  val nullable = col.nullable
  val maxValueCount = col.maxValueCount

  def position(): Int

  def clear(): Unit
}

class WriteColumnBuffer[@specialized T: ClassTag](col: ColumnBuffer[T, _]) extends RWColumnBuffer(col) {
  val values = ByteBuffer.allocateDirect(col.maxValueCount * col.valueWidth).order(ByteOrder.nativeOrder())
  private val nullValue = Array.fill[Byte](col.alignSize)(0: Byte)
  lazy val markers = ByteBuffer.allocateDirect(col.maxValueCount).order(ByteOrder.nativeOrder())

  /** Make valueType visible outside */
  val valueType = col.valueTypeIn

  def put(source: T): Unit = {
    col.put(source, values)
    if (col.nullable) {
      markers.put(NonNullMarker)
    }
  }

  def putNull(): Unit = {
    if (!col.nullable) throw new IllegalStateException(s"Cannot store NULLs in non-nullable column '${col.name}'.")
    markers.put(NullMarker)
    values.put(nullValue)
  }

  override def position(): Int = {
    var ret = values.position()
    if (col.nullable) {
      ret += markers.position()
    }
    ret
  }

  def flip(): Unit = {
    values.flip()
    if (col.nullable) {
      markers.flip()
    }
  }

  override def clear(): Unit = {
    values.clear()
    if (col.nullable) {
      markers.clear()
    }
  }
}

class ReadColumnBuffer[@specialized T: ClassTag](col: ColumnBuffer[_, T]) extends RWColumnBuffer(col) {
  private val values = Array.ofDim[T](col.maxValueCount)
  private val isNullValue = Array.ofDim[Boolean](col.maxValueCount)
  private lazy val markers = Array.ofDim[Byte](col.maxValueCount)

  private var leftPos = 0
  private var rightPos = 0

  /** Make valueType visible outside */
  val valueType = col.valueTypeOut

  private def isEmpty = leftPos >= rightPos

  def fill(source: ByteBuffer, n: Int): Unit = {
    val nLimit = Math.min(n, maxValueCount)
    if (col.nullable) {
      source.get(markers, 0, nLimit)
    }
    var pad = padding(IntSize /* messageLength, not incl. in source position */ + source.position, col.alignSize)
    source.position(source.position + pad)
    while (rightPos < nLimit) {
      isNullValue(rightPos) = if (nullable) markers(rightPos) == NullMarker else false
      values(rightPos) = col.get(source) // This returns a deserialized value to us
      rightPos += 1
    }
  }

  def get(): T = {
    if (isEmpty) throw new IllegalStateException(s"Empty buffer.")
    val ret = values(leftPos)
    leftPos = (leftPos + 1) % rightPos
    ret
  }

  def getIsNull(): Boolean = {
    if (isEmpty) throw new IllegalStateException(s"Empty buffer.")
    isNullValue(leftPos)
  }

  override def position(): Int = rightPos

  override def clear(): Unit = {
    leftPos = 0
    rightPos = 0
  }
}

/**
 * Trait to be used when implementing a companion object for a typed ColumnBuffer
 *  (e.g. object IntColumnBuffer extends ColumnBufferInstance[Int])
 */
private[colbuffer] trait ColumnBufferBuilder {
  protected def isInBounds(value: Int, bounds: (Int, Int)): Boolean = (bounds._1 <= value && value <= bounds._2)

  protected def ofDataType(t: VectorDataType.EnumVal): PartialFunction[ColumnBufferBuildParams, ColumnBufferBuildParams] = {
    case p if VectorDataType(p.tpe) == t => p
  }

  /** Get a new instance of `ColumnBuffer` for the given column type params. */
  private[colbuffer] val build: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_, _]]
}

/**
 * Case class to be used when trying to create a typed W/R column buffer object
 *  through the `newWriteBuffer` or `newReadBuffer` methods.
 *
 *  @param name the column's name
 *  @param tpe the data type's name (required in lower cases)
 *  @param precision the data type's precision
 *  @param scale the data type's scale size
 *  @param maxValueCount the size of this column buffer (in tuple/value counts)
 *  @param nullable whether this column accepts null values or not
 */
case class ColumnBufferBuildParams(name: String, tpe: String, precision: Int, scale: Int, maxValueCount: Int, nullable: Boolean)
  extends Serializable {
  require(tpe == tpe.toLowerCase, s"Column type '${tpe}' should be in lower case letters.")
}

/** This is a `Factory` implementation of `ColumnBuffers`. */
object ColumnBuffer {
  private final val colBufBuilders: List[ColumnBufferBuilder] = List(
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
    TimestampColumnBuffer)

  private val build = colBufBuilders.map(_.build).reduce(_ orElse _)

  /**
   * Get the `ColumnBuffer` object for the given `ColumnBufferBuildParams` params.
   *  @return a `ColumnBuffer` object (or throws an exception if an appropiate `ColumnBuffer` was not found)
   */
  private def apply(p: ColumnBufferBuildParams): ColumnBuffer[_, _] = PartialFunction.condOpt(p)(build) match {
    case Some(cb) => cb
    case None     => throw new Exception(s"Unable to find internal buffer for column '${p.name}' of type '${p.tpe}'")
  }

  /**
   * Get a `WriteColumnBuffer` object for the given `ColumnBufferBuildParams` params.
   */
  def newWriteBuffer(p: ColumnBufferBuildParams): WriteColumnBuffer[_] = new WriteColumnBuffer(ColumnBuffer(p))

  /**
   * Get a `ReadColumnBuffer` object for the given `ColumnBufferBuildParams` params.
   */
  def newReadBuffer(p: ColumnBufferBuildParams): ReadColumnBuffer[_] = new ReadColumnBuffer(ColumnBuffer(p))
}
