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

import java.nio.ByteBuffer
import java.math.BigDecimal

private class DecimalLongColumnBuffer(valueCount: Int, name: String, index: Int, precision: Int, scale: Int, nullable: Boolean) extends
              DecimalColumnBuffer(valueCount, LongSize, name, index, precision, scale, nullable) {

  override protected def putScaled(scaledSource: BigDecimal, buffer: ByteBuffer): Unit = {
    buffer.putLong(scaledSource.longValue())
  }
}

/** `ColumnBuffer` object for `decimal(<long>)` types. */
object DecimalLongColumnBuffer extends DecimalColumnBufferInstance {
  // scalastyle:off magic.number
  final override protected def minPrecision = 10
  final override protected def maxPrecision = 18
  // scalastyle:on magic.number

  private[colbuffer] override def getNewInstance(name: String, index: Int, precision: Int, scale: Int,
                                                 nullable: Boolean, maxRowCount: Int): ColumnBuffer[Number] = {
    new DecimalLongColumnBuffer(maxRowCount, name, index, precision, scale, nullable)
  }
}
