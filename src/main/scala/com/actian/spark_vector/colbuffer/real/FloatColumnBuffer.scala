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

private class FloatColumnBuffer(valueCount: Int, name: String, index: Int, nullable: Boolean) extends
              ColumnBuffer[Float](valueCount, FloatColumnBuffer.FloatSize, FloatColumnBuffer.FloatSize, name, index, nullable) {

  override def put(source: Float, buffer: ByteBuffer): Unit = {
    buffer.putFloat(source)
  }
}

/** `ColumnBuffer` object for `real`, `float4` types. */
object FloatColumnBuffer extends ColumnBufferInstance[Float] {
  private final val FloatSize = 4
  private final val FloatTypeId1 = "real"
  private final val FloatTypeId2 = "float4"

  private[colbuffer] override def getNewInstance(name: String, index: Int, precision: Int, scale: Int,
                                                 nullable: Boolean, maxRowCount: Int): ColumnBuffer[Float] = {
    new FloatColumnBuffer(maxRowCount, name, index, nullable)
  }

  private[colbuffer] override def supportsColumnType(tpe: String, precision: Int, scale:Int, nullable: Boolean): Boolean = {
    tpe.equalsIgnoreCase(FloatTypeId1) || tpe.equalsIgnoreCase(FloatTypeId2)
  }
}
