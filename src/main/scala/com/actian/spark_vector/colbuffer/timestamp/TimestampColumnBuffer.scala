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

import com.actian.spark_vector.colbuffer._
import com.actian.spark_vector.colbuffer.util._

import org.apache.spark.Logging

import java.math.BigInteger
import java.nio.ByteBuffer
import java.sql.Timestamp

private[colbuffer] abstract class TimestampColumnBuffer(maxValueCount: Int, valueWidth: Int, name: String, scale: Int, nullable: Boolean,
  converter: TimestampConversion.TimestampConverter, adjustToUTC: Boolean) extends ColumnBuffer[Timestamp](maxValueCount, valueWidth, valueWidth, name, nullable) {

  override protected def put(source: Timestamp, buffer: ByteBuffer): Unit = {
    if (adjustToUTC) {
      TimeConversion.convertLocalTimestampToUTC(source)
    }
    val convertedSource = converter.convert(source.getTime() / PowersOfTen(MillisecondsScale), source.getNanos(), 0, scale)
    putConverted(convertedSource, buffer)
  }

  protected def putConverted(converted: BigInteger, buffer: ByteBuffer): Unit
}

private[colbuffer] trait TimestampColumnBufferInstance extends ColumnBufferInstance {
  protected val adjustToUTC: Boolean = true
  protected def createConverter(): TimestampConversion.TimestampConverter
}

private[colbuffer] trait TimestampLZColumnBufferInstance extends TimestampColumnBufferInstance {

  protected def supportsLZColumnType(tpe: String, columnScale: Int, minScale: Int, maxScale: Int): Boolean =
    tpe.equalsIgnoreCase(TimestampLZTypeId) && minScale <= columnScale && columnScale <= maxScale

  private class TimestampLZConverter extends TimestampConversion.TimestampConverter {

    override def convert(epochSeconds: Long, subsecNanos: Long, offsetSeconds: Int, scale: Int): BigInteger =
      TimestampConversion.scaledTimestamp(epochSeconds, subsecNanos, 0, scale)
  }

  override protected def createConverter(): TimestampConversion.TimestampConverter = new TimestampLZConverter()
}

private[colbuffer] trait TimestampNZColumnBufferInstance extends TimestampColumnBufferInstance {

  protected def supportsNZColumnType(tpe: String, columnScale: Int, minScale: Int, maxScale: Int): Boolean =
    (tpe.equalsIgnoreCase(TimestampNZTypeId1) || tpe.equalsIgnoreCase(TimestampNZTypeId2)) && minScale <= columnScale && columnScale <= maxScale

  private class TimestampNZConverter extends TimestampConversion.TimestampConverter {

    override def convert(epochSeconds: Long, subsecNanos: Long, offsetSeconds: Int, scale: Int): BigInteger =
      TimestampConversion.scaledTimestamp(epochSeconds, subsecNanos, offsetSeconds, scale)
  }

  override protected def createConverter(): TimestampConversion.TimestampConverter = new TimestampNZConverter()
}

private[colbuffer] trait TimestampTZColumnBufferInstance extends TimestampColumnBufferInstance {

  protected def supportsTZColumnType(tpe: String, columnScale: Int, minScale: Int, maxScale: Int): Boolean =
    tpe.equalsIgnoreCase(TimestampTZTypeId) && minScale <= columnScale && columnScale <= maxScale

  private class TimestampTZConverter extends TimestampConversion.TimestampConverter {
    // scalastyle:off magic.number
    private final val TimeMask = new BigInteger(timeMask)
    private final val ZoneMask = BigInteger.valueOf(0x7FF)

    /** Set the 117 most significant bits to 1 and the 11 least significant bits to 0. */
    private def timeMask: Array[Byte] = {
      val mask = new Array[Byte](16)
      mask.update(0, 0.toByte)
      mask.update(1, 248.toByte)
      var i = 2
      while (i < mask.length) {
        mask.update(i, 0xFF.toByte)
        i += 1
      }
      mask
    }

    override def convert(epochSeconds: Long, subsecNanos: Long, offsetSeconds: Int, scale: Int): BigInteger = {
      val scaledTimestamp = TimestampConversion.scaledTimestamp(epochSeconds, subsecNanos, 0, scale)
      scaledTimestamp.shiftLeft(11).and(TimeMask).or(BigInteger.valueOf(offsetSeconds / SecondsInMinute).and(ZoneMask))
    }
    // scalastyle:off magic.number
  }

  override protected def createConverter(): TimestampConversion.TimestampConverter = new TimestampTZConverter()
}
