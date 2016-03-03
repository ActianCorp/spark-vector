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
package com.actian.spark_vector.colbuffer

import java.nio.ByteBuffer
import java.nio.ByteOrder
import scala.reflect._
import scala.throws._

/**
 * Abstract class to be used when implementing the class for a typed ColumnBuffer
 * (e.g. object IntColumnBuffer extends ColumnBuffer[Int])
 *
 * This class implements the base methods for buffering vectors of column values.
 * The `put`, `flip`, `clear` methods are according to the Buffer interface.
 * The column value serialization should be implemented within the concrete
 * (typed) class instead.
 */
abstract class ColumnBuffer[@specialized T: ClassTag](valueCount: Int, valueWidth: Int, val alignSize: Int,
                                                      name: String, index: Int, val nullable: Boolean) {
  private final val NullMarker = 1:Byte
  private final val NonNullMarker = 0:Byte

  val valueType = classTag[T]
  val values = ByteBuffer.allocateDirect(valueCount * valueWidth).order(ByteOrder.nativeOrder())
  val markers = ByteBuffer.allocateDirect(valueCount).order(ByteOrder.nativeOrder())
  private val nullValue = Array.fill[Byte](alignSize)(0:Byte)

  protected def put(source: T, buffer: ByteBuffer): Unit

  def put(source: T): Unit = {
    put(source, values)
    if (nullable) {
      markers.put(NonNullMarker)
    }
  }

  @throws(classOf[IllegalArgumentException])
  def putNull(): Unit = {
    if (!nullable) {
      throw new IllegalArgumentException(
                "Cannot store NULL values in a non-nullable column '" + name + "'.")
    }
    markers.put(NullMarker)
    values.put(nullValue)
  }

  def size: Int = {
    var ret = values.position()
    if (nullable) {
      ret += markers.position()
    }
    ret
  }

  def flip(): Unit = {
    values.flip()
    if (nullable) {
      markers.flip()
    }
  }

  def clear(): Unit = {
    values.clear()
    if (nullable) {
      markers.clear()
    }
  }
}
