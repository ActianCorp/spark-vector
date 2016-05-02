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
package com.actian.spark_vector.colbuffer.decimal

import com.actian.spark_vector.colbuffer._
import com.actian.spark_vector.colbuffer.util.BigIntegerConversion
import com.actian.spark_vector.vector.VectorDataType

import java.lang.Number
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer

private[colbuffer] abstract class DecimalColumnBuffer(p: ColumnBufferBuildParams, valueWidth: Int) extends
  ColumnBuffer[BigDecimal, BigDecimal](p.name, p.maxValueCount, valueWidth, valueWidth, p.nullable) {
  override def put(source: BigDecimal, buffer: ByteBuffer): Unit = put(source.unscaledValue, buffer)

  protected def put(unscaled: BigInteger, buffer: ByteBuffer): Unit

  override def get(buffer: ByteBuffer): BigDecimal
}

private class DecimalByteColumnBuffer(p: ColumnBufferBuildParams) extends DecimalColumnBuffer(p, ByteSize) {
  override protected def put(unscaled: BigInteger, buffer: ByteBuffer): Unit = buffer.put(unscaled.byteValue)

  override def get(buffer: ByteBuffer): BigDecimal = BigDecimal.valueOf(buffer.get(), p.scale)
}

private object DecimalByteColumnBuffer {
  final val PrecisionBounds = (0, 2)
}

private class DecimalShortColumnBuffer(p: ColumnBufferBuildParams) extends DecimalColumnBuffer(p, ShortSize) {
  override protected def put(unscaled: BigInteger, buffer: ByteBuffer): Unit = buffer.putShort(unscaled.shortValue)

  override def get(buffer: ByteBuffer): BigDecimal = BigDecimal.valueOf(buffer.getShort(), p.scale)
}

private object DecimalShortColumnBuffer {
  final val PrecisionBounds = (3, 4)
}

private class DecimalIntColumnBuffer(p: ColumnBufferBuildParams) extends DecimalColumnBuffer(p, IntSize) {
  override protected def put(unscaled: BigInteger, buffer: ByteBuffer): Unit = buffer.putInt(unscaled.intValue)

  override def get(buffer: ByteBuffer): BigDecimal = BigDecimal.valueOf(buffer.getInt(), p.scale)
}

private object DecimalIntColumnBuffer {
  final val PrecisionBounds = (5, 9)
}

private class DecimalLongColumnBuffer(p: ColumnBufferBuildParams) extends DecimalColumnBuffer(p, LongSize) {
  override protected def put(unscaled: BigInteger, buffer: ByteBuffer): Unit = buffer.putLong(unscaled.longValue)

  override def get(buffer: ByteBuffer): BigDecimal = BigDecimal.valueOf(buffer.getLong(), p.scale)
}

private object DecimalLongColumnBuffer {
  final val PrecisionBounds = (10, 18)
}

private class DecimalLongLongColumnBuffer(p: ColumnBufferBuildParams) extends DecimalColumnBuffer(p, LongLongSize) {
  override protected def put(unscaled: BigInteger, buffer: ByteBuffer): Unit = BigIntegerConversion.putLongLongByteArray(buffer, unscaled)

  override def get(buffer: ByteBuffer): BigDecimal = new BigDecimal(BigIntegerConversion.getLongLongByteArray(buffer), p.scale)
}

private object DecimalLongLongColumnBuffer {
  final val PrecisionBounds = (19, 38)
}

/** Builds a `ColumnBuffer` object for for `decimal(<byte, short, int, long, long long>)` types. */
private[colbuffer] object DecimalColumnBuffer extends ColumnBufferBuilder {
  private val buildPartial: PartialFunction[ColumnBufferBuildParams, ColumnBufferBuildParams] = ofDataType(VectorDataType.DecimalType) andThenPartial {
    case p if isInBounds(p.scale, (0, p.precision)) => p
  }

  override private[colbuffer] val build: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_, _]] = buildPartial andThenPartial {
    /** `ColumnBuffer` object for `decimal(<byte>)` types. */
    case p if isInBounds(p.precision, DecimalByteColumnBuffer.PrecisionBounds) => new DecimalByteColumnBuffer(p)
    /** `ColumnBuffer` object for `decimal(<short>)` types. */
    case p if isInBounds(p.precision, DecimalShortColumnBuffer.PrecisionBounds) => new DecimalShortColumnBuffer(p)
    /** `ColumnBuffer` object for `decimal(<int>)` types. */
    case p if isInBounds(p.precision, DecimalIntColumnBuffer.PrecisionBounds) => new DecimalIntColumnBuffer(p)
    /** `ColumnBuffer` object for `decimal(<long>)` types. */
    case p if isInBounds(p.precision, DecimalLongColumnBuffer.PrecisionBounds) => new DecimalLongColumnBuffer(p)
    /** `ColumnBuffer` object for `decimal(<long long>)` types. */
    case p if isInBounds(p.precision, DecimalLongLongColumnBuffer.PrecisionBounds) => new DecimalLongLongColumnBuffer(p)
  }
}
