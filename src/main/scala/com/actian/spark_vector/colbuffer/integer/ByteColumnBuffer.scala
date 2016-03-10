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
package com.actian.spark_vector.colbuffer.integer

import com.actian.spark_vector.colbuffer._

import java.nio.ByteBuffer

private class ByteColumnBuffer(maxValueCount: Int, name: String, nullable: Boolean) extends
  ColumnBuffer[Byte](maxValueCount, ByteSize, ByteSize, name, nullable) {

  override protected def put(source: Byte, buffer: ByteBuffer): Unit = buffer.put(source)
}

/** `ColumnBuffer` object for `tinyint`, `integer1` types. */
object ByteColumnBuffer extends ColumnBufferInstance {

  private[colbuffer] override def getNewInstance(name: String, precision: Int, scale: Int, nullable: Boolean, maxValueCount: Int): ColumnBuffer[_] =
    new ByteColumnBuffer(maxValueCount, name, nullable)

  private[colbuffer] override def supportsColumnType(tpe: String, precision: Int, scale:Int, nullable: Boolean): Boolean =
    tpe.equalsIgnoreCase(ByteTypeId1) || tpe.equalsIgnoreCase(ByteTypeId2)
}
