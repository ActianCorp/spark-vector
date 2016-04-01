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

import java.nio.ByteBuffer

private class BooleanColumnBuffer(p: ColumnBufferBuildParams) extends ColumnBuffer[Boolean](p.name, p.maxValueCount, BooleanSize, BooleanSize, p.nullable) {
  override protected def put(source: Boolean, buffer: ByteBuffer): Unit = source.booleanValue() match {
    case true => buffer.put(BooleanColumnBuffer.True)
    case false => buffer.put(BooleanColumnBuffer.False)
  }

  override protected def putOne(source: ByteBuffer) = ???

  override def get() = ???
}

/** Builds a `ColumnBuffer` object for `boolean` types. */
private[colbuffer] object BooleanColumnBuffer extends ColumnBufferBuilder {
  private final val True = 1:Byte
  private final val False = 0:Byte

  override private[colbuffer] val build: PartialFunction[ColumnBufferBuildParams, ColumnBuffer[_]] = {
    case p if p.tpe == BooleanTypeId => new BooleanColumnBuffer(p)
  }
}
