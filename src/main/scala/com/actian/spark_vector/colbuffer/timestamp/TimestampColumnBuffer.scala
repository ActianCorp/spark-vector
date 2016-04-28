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
package com.actian.spark_vector.colbuffer.timestamp

import org.apache.spark.sql.catalyst.util.DateTimeUtils

import org.apache.spark.Logging

import com.actian.spark_vector.colbuffer._
import com.actian.spark_vector.colbuffer.util.{ TimestampConversion, TimeConversion, BigIntegerConversion, PowersOfTen, MillisecondsScale }

import java.math.BigInteger
import java.nio.ByteBuffer
import java.sql.Timestamp

private case class TimestampColumnBufferParams(cbParams: ColumnBufferBuildParams, converter: TimestampConversion.TimestampConverter,
  adjustToUTC: Boolean = false)

private[colbuffer] abstract class TimestampColumnBuffer(p: TimestampColumnBufferParams, valueWidth: Int) extends
  ColumnBuffer[Timestamp, Long](p.cbParams.name, p.cbParams.maxValueCount, valueWidth, valueWidth, p.cbParams.nullable) with Logging {
  private val ts = new Timestamp(System.currentTimeMillis())

  override def put(source: Timestamp, buffer: ByteBuffer): Unit = {
    if (p.adjustToUTC) {
      TimeConversion.convertLocalTimestampToUTC(source)
    }
    val convertedSource = p.converter.convert(source.getTime() / PowersOfTen(MillisecondsScale), source.getNanos(), p.cbParams.scale)
    putConverted(convertedSource, buffer)
  }

  protected def putConverted(converted: BigInteger, buffer: ByteBuffer): Unit

  override def get(buffer: ByteBuffer): Long = {
    val (epochSeconds, subsecNanos) = p.converter.deconvert(getConverted(buffer), p.cbParams.scale)
    ts.setTime(epochSeconds * PowersOfTen(MillisecondsScale))
    ts.setNanos(subsecNanos.toInt)
    if (p.adjustToUTC) {
      TimeConversion.convertUTCToLocalTimestamp(ts)
    }
    DateTimeUtils.fromJavaTimestamp(ts)
  }

  protected def getConverted(buffer: ByteBuffer): BigInteger
}

private class TimestampLongColumnBuffer(p: TimestampColumnBufferParams) extends TimestampColumnBuffer(p, LongSize) {
  override protected def putConverted(converted: BigInteger, buffer: ByteBuffer): Unit = buffer.putLong(converted.longValue)

  /* TODO: remove the need of new BigInteger obj for TimestampLong */
  override protected def getConverted(buffer: ByteBuffer): BigInteger = BigInteger.valueOf(buffer.getLong())
}

private class TimestampLongLongColumnBuffer(p: TimestampColumnBufferParams) extends TimestampColumnBuffer(p, LongLongSize) {
  override protected def putConverted(converted: BigInteger, buffer: ByteBuffer): Unit = BigIntegerConversion.putLongLongByteArray(buffer, converted)

  override protected def getConverted(buffer: ByteBuffer): BigInteger = BigIntegerConversion.getLongLongByteArray(buffer)
}

private class TimestampNZConverter extends TimestampConversion.TimestampConverter {
  override def convert(epochSeconds: Long, subsecNanos: Long, scale: Int): BigInteger = TimestampConversion.scaleTimestamp(epochSeconds, subsecNanos, scale)

  override def deconvert(convertedSource: BigInteger, scale: Int): (Long, Long) = TimestampConversion.unscaleTimestamp(convertedSource, scale)
}

private class TimestampTZConverter extends TimestampConversion.TimestampConverter {
  // scalastyle:off magic.number
  private final val TimeMask = new BigInteger(timeMask)
  private final val ZoneMask = BigInteger.valueOf(0x7FF)

  /** Set the 117 most significant bits to 1 and the 11 least significant bits to 0. */
  private def timeMask: Array[Byte] = {
    val mask = new Array[Byte](LongLongSize)
    mask.update(0, 0.toByte)
    mask.update(1, 248.toByte)
    var i = 2
    while (i < mask.length) {
      mask.update(i, 0xFF.toByte)
      i += 1
    }
    mask
  }

  override def convert(epochSeconds: Long, subsecNanos: Long, scale: Int): BigInteger = {
    val scaledNanos = TimestampConversion.scaleTimestamp(epochSeconds, subsecNanos, scale)
    scaledNanos.shiftLeft(11).and(TimeMask).or(BigInteger.ZERO.and(ZoneMask))
  }

  override def deconvert(convertedSource: BigInteger, scale: Int): (Long, Long) = {
    val deconvertedSource = convertedSource.shiftRight(11)
    TimestampConversion.unscaleTimestamp(deconvertedSource, scale)
  }
  // scalastyle:on magic.number
}

private class TimestampLZConverter extends TimestampNZConverter {
  override def convert(epochSeconds: Long, subsecNanos: Long, scale: Int): BigInteger =
    super.convert(epochSeconds, subsecNanos, scale)
}

/** Builds a `ColumnBuffer` object for `timestamp` (NZ, TZ, LZ) types. */
private[colbuffer] object TimestampColumnBuffer extends ColumnBufferBuilder {
  private final val (nzlzIntScaleBounds, nzlzLongScaleBounds) = ((0, 7), (8, 9))
  private final val (tzIntScaleBounds, tzLongScaleBounds) = ((0, 4), (5, 9))

  private val buildNZPartial: PartialFunction[ColumnBufferBuildParams, TimestampColumnBufferParams] = {
    case p if p.tpe == TimestampNZTypeId1 || p.tpe == TimestampNZTypeId2 => TimestampColumnBufferParams(p, new TimestampNZConverter(), true)
  }

  private val buildLZPartial: PartialFunction[ColumnBufferBuildParams, TimestampColumnBufferParams] = {
    case p if p.tpe == TimestampLZTypeId => TimestampColumnBufferParams(p, new TimestampLZConverter())
  }

  private val buildNZLZ: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_, _]] = (buildNZPartial orElse buildLZPartial) andThenPartial {
    case nzlz if isInBounds(nzlz.cbParams.scale, nzlzIntScaleBounds) => new TimestampLongColumnBuffer(nzlz)
    case nzlz if isInBounds(nzlz.cbParams.scale, nzlzLongScaleBounds) => new TimestampLongLongColumnBuffer(nzlz)
  }

  private val buildTZPartial: PartialFunction[ColumnBufferBuildParams, TimestampColumnBufferParams] = {
    case p if p.tpe == TimestampTZTypeId => TimestampColumnBufferParams(p, new TimestampTZConverter())
  }

  private val buildTZ: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_, _]] = buildTZPartial andThenPartial {
    case tz if isInBounds(tz.cbParams.scale, tzIntScaleBounds) => new TimestampLongColumnBuffer(tz)
    case tz if isInBounds(tz.cbParams.scale, tzLongScaleBounds) => new TimestampLongLongColumnBuffer(tz)
  }

  override private[colbuffer] val build: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_, _]] = buildNZLZ orElse buildTZ
}
