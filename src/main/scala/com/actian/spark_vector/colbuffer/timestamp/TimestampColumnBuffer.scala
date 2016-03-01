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
import com.actian.spark_vector.colbuffer.util.TimeConversion
import com.actian.spark_vector.colbuffer.util.TimestampConversion

import org.apache.spark.Logging

import java.math.BigInteger
import java.nio.ByteBuffer
import java.sql.Timestamp

private[colbuffer] abstract class TimestampColumnBuffer(valueCount: Int, valueWidth: Int, name: String, index: Int, scale: Int, nullable: Boolean,
                                                        converter: TimestampConversion.TimestampConverter, adjustToUTC: Boolean) extends
                                  ColumnBuffer[Timestamp](valueCount, valueWidth, valueWidth, name, index, nullable) with Logging {

  override protected def put(source: Timestamp, buffer: ByteBuffer): Unit = {
    if (adjustToUTC) {
      TimeConversion.convertLocalTimestampToUTC(source)
    }
    val convertedSource = converter.convert(source.getTime() / 1000, source.getNanos(), 0, scale)
    logDebug(s"Trying to serialize value ${source} with time ${source.getTime()} to ${convertedSource}")
    putConverted(convertedSource, buffer)
  }

  protected def putConverted(converted: BigInteger, buffer: ByteBuffer): Unit
}

private[colbuffer] trait TimestampColumnBufferInstance extends ColumnBufferInstance[Timestamp] {
  protected def adjustToUTC: Boolean = true
  protected def createConverter(): TimestampConversion.TimestampConverter
}

private[colbuffer] trait TimestampLZColumnBufferInstance extends TimestampColumnBufferInstance {
  private final val TIMESTAMP_LZ_TYPE_ID = "timestamp with local time zone"

  protected def supportsLZColumnType(tpe: String, columnScale: Int, minScale: Int, maxScale: Int): Boolean = {
    tpe.equalsIgnoreCase(TIMESTAMP_LZ_TYPE_ID) && minScale <= columnScale && columnScale <= maxScale
  }

  private class TimestampLZConverter extends TimestampConversion.TimestampConverter {
    override def convert(epochSeconds: Long, subsecNanos: Long, offsetSeconds: Int, scale: Int): BigInteger = {
      TimestampConversion.scaledTimestamp(epochSeconds, subsecNanos, 0, scale)
    }
  }

  override protected def createConverter(): TimestampConversion.TimestampConverter = {
    new TimestampLZConverter()
  }
}

private[colbuffer] trait TimestampNZColumnBufferInstance extends TimestampColumnBufferInstance {
  private final val TIMESTAMP_NZ_TYPE_ID_1 = "timestamp"
  private final val TIMESTAMP_NZ_TYPE_ID_2 = "timestamp without time zone"

  protected def supportsNZColumnType(tpe: String, columnScale: Int, minScale: Int, maxScale: Int): Boolean = {
    (tpe.equalsIgnoreCase(TIMESTAMP_NZ_TYPE_ID_1) || tpe.equalsIgnoreCase(TIMESTAMP_NZ_TYPE_ID_2)) &&
    minScale <= columnScale && columnScale <= maxScale
  }

  private class TimestampNZConverter extends TimestampConversion.TimestampConverter {
    override def convert(epochSeconds: Long, subsecNanos: Long, offsetSeconds: Int, scale: Int): BigInteger = {
      TimestampConversion.scaledTimestamp(epochSeconds, subsecNanos, offsetSeconds, scale)
    }
  }

  override protected def createConverter(): TimestampConversion.TimestampConverter = {
    new TimestampNZConverter()
  }
}

private[colbuffer] trait TimestampTZColumnBufferInstance extends TimestampColumnBufferInstance {
  private final val TIMESTAMP_TZ_TYPE_ID = "timestamp with time zone"

  protected def supportsTZColumnType(tpe: String, columnScale: Int, minScale: Int, maxScale: Int): Boolean = {
    tpe.equalsIgnoreCase(TIMESTAMP_TZ_TYPE_ID) && minScale <= columnScale && columnScale <= maxScale
  }

  private class TimestampTZConverter extends TimestampConversion.TimestampConverter {
    private final val TIME_MASK = new BigInteger(timeMask)
    private final val ZONE_MASK = BigInteger.valueOf(0x7FF)

    /** Set the 117 most significant bits to 1 and the 11 least significant bits to 0. */
    private def timeMask: Array[Byte] = {
      val mask = new Array[Byte](16)
      mask.update(0, 0.toByte)
      mask.update(1, 248.toByte)
      for (i <- 2 to mask.length - 1) {
        mask.update(i, 0xFF.toByte)
      }
      mask
    }

    override def convert(epochSeconds: Long, subsecNanos: Long, offsetSeconds: Int, scale: Int): BigInteger = {
      val scaledTimestamp = TimestampConversion.scaledTimestamp(epochSeconds, subsecNanos, 0, scale)
      scaledTimestamp.shiftLeft(11).and(TIME_MASK).or(BigInteger.valueOf(offsetSeconds / TimestampConversion.SECONDS_IN_MINUTE).and(ZONE_MASK))
    }
  }

  override protected def createConverter(): TimestampConversion.TimestampConverter = {
    new TimestampTZConverter()
  }
}
