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
package com.actian.spark_vector.colbuffer.time

import com.actian.spark_vector.colbuffer._
import com.actian.spark_vector.colbuffer.util.TimeConversion

import org.apache.spark.Logging

import java.nio.ByteBuffer
import java.sql.Timestamp

private[colbuffer] abstract class TimeColumnBuffer(maxValueCount: Int, valueWidth: Int, name: String, scale: Int, nullable: Boolean,
  converter: TimeConversion.TimeConverter, adjustToUTC: Boolean) extends ColumnBuffer[Timestamp](maxValueCount, valueWidth, valueWidth, name, nullable) {

  override protected def put(source: Timestamp, buffer: ByteBuffer): Unit = {
    if (adjustToUTC) {
      TimeConversion.convertLocalTimestampToUTC(source)
    }
    val convertedSource = converter.convert(TimeConversion.normalizedTime(source), scale)
    putConverted(convertedSource, buffer)
  }

  protected def putConverted(converted: Long, buffer: ByteBuffer): Unit
}

private[colbuffer] trait TimeColumnBufferInstance extends ColumnBufferInstance {
  protected val adjustToUTC: Boolean = true
  protected def createConverter(): TimeConversion.TimeConverter
}

private[colbuffer] trait TimeLZColumnBufferInstance extends TimeColumnBufferInstance {

  protected def supportsLZColumnType(tpe: String, columnScale: Int, minScale: Int, maxScale: Int): Boolean =
    tpe.equalsIgnoreCase(TimeLZTypeId) && minScale <= columnScale && columnScale <= maxScale

  private class TimeLZConverter extends TimeConversion.TimeConverter {

    override def convert(nanos: Long, scale: Int): Long = TimeConversion.scaledTime(nanos, scale)
  }

  override protected def createConverter(): TimeConversion.TimeConverter = new TimeLZConverter()
}

private[colbuffer] trait TimeNZColumnBufferInstance extends TimeColumnBufferInstance {

  protected def supportsNZColumnType(tpe: String, columnScale: Int, minScale: Int, maxScale: Int): Boolean =
    (tpe.equalsIgnoreCase(TimeNZTypeId1) || tpe.equalsIgnoreCase(TimeNZTypeId2)) && minScale <= columnScale && columnScale <= maxScale

  private class TimeNZConverter extends TimeConversion.TimeConverter {

    override def convert(nanos: Long, scale: Int): Long = TimeConversion.scaledTime(nanos, scale)
  }

  override protected def createConverter(): TimeConversion.TimeConverter = new TimeNZConverter()
}

private[colbuffer] trait TimeTZColumnBufferInstance extends TimeColumnBufferInstance {

  protected def supportsTZColumnType(tpe: String, columnScale: Int, minScale: Int, maxScale: Int): Boolean = {
    tpe.equalsIgnoreCase(TimeTZTypeId) && minScale <= columnScale && columnScale <= maxScale
  }

  private class TimeTZConverter extends TimeConversion.TimeConverter {
    private final val TimeMask = 0xFFFFFFFFFFFFF800L

    override def convert(nanos: Long, scale: Int): Long = ((TimeConversion.scaledTime(nanos, scale) << 11) & (TimeMask))
  }

  override protected def createConverter(): TimeConversion.TimeConverter = new TimeTZConverter()
}
