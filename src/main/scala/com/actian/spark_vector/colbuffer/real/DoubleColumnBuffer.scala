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
package com.actian.spark_vector.colbuffer.real

import com.actian.spark_vector.colbuffer._

import java.nio.ByteBuffer

private class DoubleColumnBuffer(valueCount: Int, name: String, index: Int, nullable: Boolean) extends
              ColumnBuffer[Double](valueCount, DoubleColumnBuffer.DOUBLE_SIZE, DoubleColumnBuffer.DOUBLE_SIZE, name, index, nullable) {

  override protected def put(source: Double, buffer: ByteBuffer): Unit = {
    buffer.putDouble(source)
  }
}

/** `ColumnBuffer` object for `float`, `float8`, `double precision` types. */
object DoubleColumnBuffer extends ColumnBufferInstance[Double] {
  private final val DOUBLE_SIZE = 8
  private final val DOUBLE_TYPE_ID_1 = "float"
  private final val DOUBLE_TYPE_ID_2 = "float8";
  private final val DOUBLE_TYPE_ID_3 = "double precision"

  private[colbuffer] override def getNewInstance(name: String, index: Int, precision: Int, scale: Int,
                                                 nullable: Boolean, maxRowCount: Int): ColumnBuffer[Double] = {
    new DoubleColumnBuffer(maxRowCount, name, index, nullable)
  }

  private[colbuffer] override def supportsColumnType(tpe: String, precision: Int, scale:Int, nullable: Boolean): Boolean = {
    tpe.equalsIgnoreCase(DOUBLE_TYPE_ID_1) || tpe.equalsIgnoreCase(DOUBLE_TYPE_ID_2) || tpe.equalsIgnoreCase(DOUBLE_TYPE_ID_3)
  }
}
