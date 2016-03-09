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
package com.actian.spark_vector.colbuffer.singles

import com.actian.spark_vector.colbuffer._
import com.actian.spark_vector.colbuffer.util._

import java.nio.ByteBuffer
import java.sql.Date

private class DateColumnBuffer(maxValueCount: Int, name: String, nullable: Boolean) extends
  ColumnBuffer[Date](maxValueCount, DateSize, DateSize, name, nullable) {

  override protected def put(source: Date, buffer: ByteBuffer): Unit = {
    TimeConversion.convertLocalDateToUTC(source)
    buffer.putInt((source.getTime() / MillisecondsInDay + DateColumnBuffer.DaysBeforeEpoch).toInt)
  }
}

/** `ColumnBuffer` object for `ansidate` types. */
object DateColumnBuffer extends ColumnBufferInstance {
  private final val DaysBeforeEpoch = 719528

  private[colbuffer] override def getNewInstance(name: String, precision: Int, scale: Int, nullable: Boolean, maxValueCount: Int): ColumnBuffer[_] =
    new DateColumnBuffer(maxValueCount, name, nullable)

  private[colbuffer] override def supportsColumnType(tpe: String, precision: Int, scale:Int, nullable: Boolean): Boolean =
    tpe.equalsIgnoreCase(DateTypeId)
}
