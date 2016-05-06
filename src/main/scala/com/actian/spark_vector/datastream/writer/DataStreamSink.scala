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

import com.actian.spark_vector.datastream.{ padding, DataStreamConnector }
import com.actian.spark_vector.colbuffer.WriteColumnBuffer

/** The `VectorSink` that flushes `ByteBuffer`s to a `Vector DataStream`'s socket */
private[writer] case class DataStreamSink(implicit socket: SocketChannel) extends Serializable {
  import DataStreamWriter._

  private final val BinaryDataCode = 5 /* X100CPT_BINARY_DATA_V2 */

  /**
   * The write position (how many bytes have already been written to `socket`). Used to
   * calculate data type alignments
   */
  private var pos: Int = 0

  private def writeDataHeader(len: Int, numTuples: Int): Unit = {
    writeInt(len)
    writeInt(BinaryDataCode)
    writeInt(numTuples) // write actual number of tuples
    pos = DataStreamConnector.DataHeaderSize
  }

  private def writeDataColumn(cb: WriteColumnBuffer[_]): Unit = {
    align(cb.alignSize)
    writeByteBufferNoFlip(cb.values)
    pos += cb.values.limit()
    if (cb.nullable) {
      writeByteBufferNoFlip(cb.markers)
      pos += cb.markers.limit()
    }
  }

  private def align(size: Int): Unit = padding(pos, size) match {
    case x if x > 0 =>
      writeByteBufferNoFlip(ByteBuffer.allocateDirect(x))
      pos += x
    case _ =>
  }

  /** Writes buffered data to the socket, the header and the actual binary data */
  def write(len: Int, numTuples: Int, columnBufs: Seq[WriteColumnBuffer[_]]): Unit = {
    writeDataHeader(len, numTuples)
    columnBufs.foreach {
      case cb =>
        cb.flip()
        writeDataColumn(cb)
        cb.clear()
    }
  }
}
