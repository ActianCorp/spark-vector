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
package com.actian.spark_vector.colbuffer.abstracts

import com.actian.spark_vector.colbuffer._
import com.actian.spark_vector.colbuffer.util._
import com.actian.spark_vector.vector.VectorDataType

import org.apache.spark.unsafe.types.UTF8String

import java.math.BigInteger
import java.nio.ByteBuffer

private[colbuffer] abstract class AbstractStringColumnBuffer(p: ColumnBufferBuildParams, valueWidth: Int) 
    extends ColumnBuffer[String, UTF8String](p.name, p.maxValueCount, valueWidth, valueWidth, p.nullable) {
  override def put(source: String, buffer: ByteBuffer): Unit
  
  override def get(buffer: ByteBuffer): UTF8String
}

private class IntervalYTMStringColumnBuffer(p: ColumnBufferBuildParams) 
    extends AbstractStringColumnBuffer(p, IntSize) {
  override def put(source: String, buffer: ByteBuffer): Unit = {
    buffer.putInt(IntervalConversion.convertIntervalYM(source))
  }
  
  override def get(buffer: ByteBuffer): UTF8String = {
    UTF8String.fromString(IntervalConversion.deconvertIntervalYM(buffer.getInt))
  }
}

private class IntervalDTSStringColumnBuffer(p: ColumnBufferBuildParams)
    extends AbstractStringColumnBuffer(p, LongSize) {
  
  override def put(source: String, buffer: ByteBuffer): Unit = {
    buffer.putLong(IntervalConversion.convertIntervalDS(source, p.scale).longValue())
  }
  
  override def get(buffer: ByteBuffer): UTF8String = {
    UTF8String.fromString(IntervalConversion.deconvertIntervalDS(BigInteger.valueOf(buffer.getLong()), p.scale))
  }
}

private class LongIntervalDTSStringColumnBuffer(p: ColumnBufferBuildParams)
    extends AbstractStringColumnBuffer(p, LongLongSize) {
  
  override def put(source: String, buffer: ByteBuffer): Unit = {
    BigIntegerConversion.putLongLongByteArray(buffer, IntervalConversion.convertIntervalDS(source, p.scale))
  }
  
  override def get(buffer: ByteBuffer): UTF8String = {
    val byteArray = Array.fill[Byte](LongLongSize)(0: Byte)
    UTF8String.fromString(IntervalConversion.deconvertIntervalDS(BigIntegerConversion.getLongLongByteArray(buffer, byteArray), p.scale))
  }
}

private class IPv4StringColumnBuffer(p: ColumnBufferBuildParams)
    extends AbstractStringColumnBuffer(p, IntSize) {
  override def put(source: String, buffer: ByteBuffer): Unit = {
    buffer.putInt(IPConversion.ipv4StringToInteger(source))
  }
  
  override def get(buffer: ByteBuffer): UTF8String = {
    UTF8String.fromString(IPConversion.ipv4IntegerToString(buffer.getInt()))
  }
}

private class IPv6StringColumnBuffer(p: ColumnBufferBuildParams)
    extends AbstractStringColumnBuffer(p, LongLongSize) {
  override def put(source: String, buffer: ByteBuffer): Unit = {
    val (lower, upper) = IPConversion.ipv6StringToLongs(source)
    buffer.putLong(lower)
    buffer.putLong(upper)
  }
  
  override def get(buffer: ByteBuffer): UTF8String = {
    UTF8String.fromString(IPConversion.ipv6LongsToString(buffer.getLong, buffer.getLong))
  }
}

private class UUIDStringColumnBuffer(p: ColumnBufferBuildParams)
    extends AbstractStringColumnBuffer(p, LongLongSize) {
  override def put(source: String, buffer: ByteBuffer): Unit = {
    val (lower, upper) = UUIDConversion.convertUUID(source)
    buffer.putLong(lower)
    buffer.putLong(upper)
  }
  
  override def get(buffer: ByteBuffer): UTF8String = {
    UTF8String.fromString(UUIDConversion.deconvertUUID(buffer.getLong(), buffer.getLong()))
  }
}

/** Builds a `ColumnBuffer` object for `interval year to month`, `interval day to second`, `ipv4`, `ipv6`, `uuid` abstract types. */
private[colbuffer] object AbstractStringColumnBuffer extends ColumnBufferBuilder {
  private val buildLongIntervalPartial: PartialFunction[ColumnBufferBuildParams, ColumnBufferBuildParams] = {
    case p if p.scale > 7 => p
  }
  
  private val buildShortIntervalPartial: PartialFunction[ColumnBufferBuildParams, ColumnBufferBuildParams] = {
    case p if p.scale >= 0 => p
  }
  
  private val buildLongInterval: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_, _]] = buildLongIntervalPartial andThenPartial {
    (ofDataType(VectorDataType.IntervalDayToSecondType) andThen { new LongIntervalDTSStringColumnBuffer(_) })
  }
  
  private val buildShortInterval: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_, _]] = buildShortIntervalPartial andThenPartial {
    (ofDataType(VectorDataType.IntervalYearToMonthType) andThen { new IntervalYTMStringColumnBuffer(_) }) orElse 
    (ofDataType(VectorDataType.IntervalDayToSecondType) andThen { new IntervalDTSStringColumnBuffer(_) })
  }
  
  private val buildIPv4: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_, _]] = {
    (ofDataType(VectorDataType.IPV4Type) andThen { new IPv4StringColumnBuffer(_) })
  }
  
  private val buildIPv6: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_, _]] = {
    (ofDataType(VectorDataType.IPV6Type) andThen { new IPv6StringColumnBuffer(_) })
  }
  
  private val buildUUID: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_, _]] = {
    (ofDataType(VectorDataType.UUIDType) andThen { new UUIDStringColumnBuffer(_) })
  }
  
  override private[colbuffer] val build: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_, _]] = buildLongInterval orElse
                                                                                                        buildShortInterval orElse
                                                                                                        buildIPv4 orElse
                                                                                                        buildIPv6 orElse
                                                                                                        buildUUID
}