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
package com.actian.spark_vector.writer

import com.actian.spark_vector.colbuffer._

import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

/** The `VectorSink` that flushes `ByteBuffers` through the `SocketChannel` `socket` to a `Vector DataStream` */
case class DataStreamSink(implicit socket: SocketChannel) extends VectorSink {
  import DataStreamWriter._
  /**
   * The write position (how many bytes have already been written to `socket`). Used to
   * calculate data type alignments
   */
  var pos: Int = 0

  private def writeColumn(values: ByteBuffer, markers: ByteBuffer, alignSize: Int, nullable: Boolean): Unit = {
    if (nullable) {
      writeByteBufferNoFlip(markers)
      pos = pos + markers.limit()
    }
    align(alignSize)
    writeByteBufferNoFlip(values)
    pos = pos + values.limit()
  }

  private def align(size: Int): Unit = {
    RowWriter.padding(pos, size) match {
      case x if x > 0 =>
        writeByteBufferNoFlip(ByteBuffer.allocateDirect(x))
        pos = pos + x
      case _ =>
    }
  }

  // scalastyle:off magic.number
  override def write(columnBuf: ColumnBuffer[_]): Unit = {
    columnBuf.flip()
    writeColumn(columnBuf.values, columnBuf.markers, columnBuf.alignSize, columnBuf.nullable)
    columnBuf.clear()
  }
  // scalastyle:on magic.number
}
