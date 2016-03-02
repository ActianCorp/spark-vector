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
import com.actian.spark_vector.colbuffer.util.TimestampConversion

import java.nio.ByteBuffer
import java.math.BigInteger
import java.sql.Timestamp

private class TimestampLongColumnBuffer(valueCount: Int, name: String, index: Int, scale: Int, nullable: Boolean,
                                        converter: TimestampConversion.TimestampConverter, adjustToUTC: Boolean) extends
              TimestampColumnBuffer(valueCount, TimestampLongColumnBuffer.TIMESTAMP_LONG_SIZE, name, index, scale, nullable, converter, adjustToUTC) {

  override protected def putConverted(converted: BigInteger, buffer: ByteBuffer): Unit = {
    buffer.putLong(converted.longValue())
  }
}

private object TimestampLongColumnBuffer {
  private final val TIMESTAMP_LONG_SIZE = 8
}

private[colbuffer] trait TimestampLongColumnBufferInstance extends TimestampColumnBufferInstance {

  private[colbuffer] override def getNewInstance(name: String, index: Int, precision: Int, scale: Int,
                                                 nullable: Boolean, maxRowCount: Int): ColumnBuffer[Timestamp] = {
     new TimestampLongColumnBuffer(maxRowCount, name, index, scale, nullable, createConverter(), adjustToUTC)
  }
}
