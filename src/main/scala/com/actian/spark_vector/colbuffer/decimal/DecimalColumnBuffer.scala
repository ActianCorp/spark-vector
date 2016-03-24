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

import java.lang.Number
import java.math.BigDecimal
import java.nio.ByteBuffer

private[colbuffer] abstract class DecimalColumnBuffer(p: ColumnBufferBuildParams, valueWidth: Int) extends
  ColumnBuffer[Number](p.name, p.maxValueCount, valueWidth, valueWidth, p.nullable) {
  override def put(source: Number, buffer: ByteBuffer): Unit = putScaled(movePoint(new BigDecimal(source.toString()), p.precision, p.scale), buffer)

  protected def putScaled(scaledSource: BigDecimal, buffer: ByteBuffer): Unit

  private def movePoint(value: BigDecimal, precision: Int, scale: Int): BigDecimal = {
    val sourceIntegerDigits = value.precision() - value.scale()
    val targetIntegerDigits = precision - scale
    val moveRightBy = if (sourceIntegerDigits > targetIntegerDigits) {
      scale - (sourceIntegerDigits - targetIntegerDigits)
    } else {
      scale
    }
    value.movePointRight(moveRightBy)
  }
}

private class DecimalByteColumnBuffer(p: ColumnBufferBuildParams) extends DecimalColumnBuffer(p, ByteSize) {
  override protected def putScaled(scaledSource: BigDecimal, buffer: ByteBuffer): Unit = buffer.put(scaledSource.byteValue())
}

private object DecimalByteColumnBuffer {
  final val PrecisionBounds = (0, 2)
}

private class DecimalShortColumnBuffer(p: ColumnBufferBuildParams) extends DecimalColumnBuffer(p, ShortSize) {
  override protected def putScaled(scaledSource: BigDecimal, buffer: ByteBuffer): Unit = buffer.putShort(scaledSource.shortValue())
}

private object DecimalShortColumnBuffer {
  final val PrecisionBounds = (3, 4)
}

private class DecimalIntColumnBuffer(p: ColumnBufferBuildParams) extends DecimalColumnBuffer(p, IntSize) {
  override protected def putScaled(scaledSource: BigDecimal, buffer: ByteBuffer): Unit = buffer.putInt(scaledSource.intValue())
}

private object DecimalIntColumnBuffer {
  final val PrecisionBounds = (5, 9)
}

private class DecimalLongColumnBuffer(p: ColumnBufferBuildParams) extends DecimalColumnBuffer(p, LongSize) {
  override protected def putScaled(scaledSource: BigDecimal, buffer: ByteBuffer): Unit = buffer.putLong(scaledSource.longValue())
}

private object DecimalLongColumnBuffer {
  final val PrecisionBounds = (10, 18)
}

private class DecimalLongLongColumnBuffer(p: ColumnBufferBuildParams) extends DecimalColumnBuffer(p, LongLongSize) {
  override protected def putScaled(scaledSource: BigDecimal, buffer: ByteBuffer): Unit =
    buffer.put(BigIntegerConversion.toLongLongByteArray(scaledSource.toBigInteger()))
}

private object DecimalLongLongColumnBuffer {
  final val PrecisionBounds = (19, 38)
}

/** Builds a `ColumnBuffer` object for for `decimal(<byte, short, int, long, long long>)` types. */
private[colbuffer] object DecimalColumnBuffer extends ColumnBufferBuilder {
  private val buildPartial: PartialFunction[ColumnBufferBuildParams, ColumnBufferBuildParams] = {
    case p if p.tpe == DecimalTypeId && isInBounds(p.scale, (0, p.precision)) => p
  }

  override private[colbuffer] val build: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_]] = buildPartial andThenPartial {
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
