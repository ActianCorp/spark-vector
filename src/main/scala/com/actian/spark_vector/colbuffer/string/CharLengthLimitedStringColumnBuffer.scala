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
package com.actian.spark_vector.colbuffer.string

import com.actian.spark_vector.colbuffer._
import com.actian.spark_vector.colbuffer.util.StringConversion

private class CharLengthLimitedStringColumnBuffer(valueCount: Int, name: String, index: Int, precision:Int, scale: Int, nullable: Boolean) extends
              ByteEncodedStringColumnBuffer(valueCount, name, index, precision * CharLengthLimitedStringColumnBuffer.MAX_UTF_8_CHAR_SIZE, scale, nullable) {

  override protected def encode(str: String): Array[Byte] = {
    StringConversion.truncateToUTF16CodeUnits(str, precision)
  }
}

private object CharLengthLimitedStringColumnBuffer {
  private final val MAX_UTF_8_CHAR_SIZE = 4
}

private[colbuffer] trait CharLengthLimitedStringColumnBufferInstance extends ColumnBufferInstance[String] {

  private[colbuffer] override def getNewInstance(name: String, index: Int, precision: Int, scale: Int,
                                                 nullable: Boolean, maxRowCount: Int): ColumnBuffer[String] = {
    new CharLengthLimitedStringColumnBuffer(maxRowCount, name, index, precision, scale, nullable)
  }
}
