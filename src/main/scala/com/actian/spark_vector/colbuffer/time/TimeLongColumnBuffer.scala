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

import java.nio.ByteBuffer
import java.sql.Timestamp

private class TimeLongColumnBuffer(valueCount: Int, name: String, index: Int, scale: Int, nullable: Boolean,
                                   converter: TimeConversion.TimeConverter, adjustToUTC: Boolean) extends
              TimeColumnBuffer(valueCount, TimeLongColumnBuffer.TIME_LONG_SIZE, name, index, scale, nullable, converter, adjustToUTC) {

  override protected def putConverted(converted: Long, buffer: ByteBuffer): Unit = {
    buffer.putLong(converted)
  }
}

private object TimeLongColumnBuffer {
  private final val TIME_LONG_SIZE = 8
}

private[colbuffer] trait TimeLongColumnBufferInstance extends TimeColumnBufferInstance {

  private[colbuffer] override def getNewInstance(name: String, index: Int, precision: Int, scale: Int,
                                                 nullable: Boolean, maxRowCount: Int): ColumnBuffer[Timestamp] = {
     new TimeLongColumnBuffer(maxRowCount, name, index, scale, nullable, createConverter(), adjustToUTC)
  }
}
