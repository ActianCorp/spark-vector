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

abstract class ColumnBuffer[@specialized T: ClassTag](valueCount: Int, valueWidth: Int, val alignSize: Int,
                                                      name:String, index:Int, val nullable: Boolean) {
  private final val NULL_MARKER = 1:Byte
  private final val NON_NULL_MARKER = 0:Byte

  val valueType = classTag[T]
  val values = ByteBuffer.allocateDirect(valueCount * valueWidth).order(ByteOrder.nativeOrder())
  val markers = ByteBuffer.allocateDirect(valueCount).order(ByteOrder.nativeOrder())
  private val nullValue = Array.fill[Byte](alignSize)(0:Byte)

  protected def put(source: T, buffer: ByteBuffer): Unit

  def put(source: T): Unit = {
    put(source, values)
    if (nullable) {
      markers.put(NON_NULL_MARKER)
    }
  }

  @throws(classOf[IllegalArgumentException])
  def putNull(): Unit = {
    if (!nullable) {
      throw new IllegalArgumentException(
                "Cannot store NULL values in a non-nullable column '" + name + "'.")
    }
    markers.put(NULL_MARKER)
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
