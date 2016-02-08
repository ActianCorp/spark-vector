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
package com.actian.spark_vectorh.writer

import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

import com.actian.spark_vectorh.buffer.VectorSink

/** The `VectorSink` that flushes `ByteBuffers` through the `SocketChannel` `socket` to a `Vector(H) DataStream` */
case class DataStreamSink(implicit socket: SocketChannel) extends VectorSink {
  import DataStreamWriter._
  /**
   * The write position (how many bytes have already been written to `socket`). Used to
   * calculate data type alignments
   */
  var pos: Int = 0

  private def writeColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer, align_size: Int): Unit = {
    if (markers != null) {
      writeByteBufferNoFlip(markers)
      pos = pos + markers.limit()
    }
    align(align_size)
    writeByteBufferNoFlip(values)
    pos = pos + values.limit()
  }

  private def align(typeSize: Int): Unit = {
    RowWriter.padding(pos, typeSize) match {
      case x if x > 0 =>
        writeByteBufferNoFlip(ByteBuffer.allocateDirect(x))
        pos = pos + x
      case _ =>
    }
  }

  // scalastyle:off magic.number
  def writeByteColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer): Unit =
    writeColumn(columnIndex, values, markers, 1)

  def writeShortColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer): Unit =
    writeColumn(columnIndex, values, markers, 2)

  def writeIntColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer): Unit =
    writeColumn(columnIndex, values, markers, 4)

  def writeLongColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer): Unit =
    writeColumn(columnIndex, values, markers, 8)

  def write128BitColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer): Unit =
    writeColumn(columnIndex, values, markers, 16)

  def writeFloatColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer): Unit =
    writeColumn(columnIndex, values, markers, 4)

  def writeDoubleColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer): Unit =
    writeColumn(columnIndex, values, markers, 8)

  def writeStringColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer): Unit =
    writeColumn(columnIndex, values, markers, 1)
  // scalastyle:on magic.number
}
