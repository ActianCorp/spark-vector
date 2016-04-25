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
package com.actian.spark_vector.datastream.writer

import java.nio.ByteBuffer
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

import com.actian.spark_vector.datastream.padding
import com.actian.spark_vector.colbuffer.WriteColumnBuffer

/** The `VectorSink` that flushes `ByteBuffers` through the `SocketChannel` `socket` to a `Vector DataStream` */
private[writer] case class DataStreamSink(implicit socket: SocketChannel) extends Serializable {
  import DataStreamWriter._

  /**
   * The write position (how many bytes have already been written to `socket`). Used to
   * calculate data type alignments
   */
  var pos: Int = 0

  private def writeColumn(values: ByteBuffer, markers: ByteBuffer, alignSize: Int, nullable: Boolean): Unit = {
    align(alignSize)
    writeByteBufferNoFlip(values)
    pos = pos + values.limit()
    if (nullable) {
      writeByteBufferNoFlip(markers)
      pos = pos + markers.limit()
    }
  }

  private def align(size: Int): Unit = padding(pos, size) match {
    case x if x > 0 =>
      writeByteBufferNoFlip(ByteBuffer.allocateDirect(x))
      pos = pos + x
    case _ =>
  }

  def write(columnBuf: WriteColumnBuffer[_]): Unit = {
    columnBuf.flip()
    writeColumn(columnBuf.values, columnBuf.markers, columnBuf.alignSize, columnBuf.nullable)
    columnBuf.clear()
  }
}
