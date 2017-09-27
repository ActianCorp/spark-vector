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
package com.actian.spark_vector.datastream.reader

import scala.annotation.tailrec
import scala.concurrent.Future

import java.nio.{ ByteOrder, ByteBuffer }
import java.nio.channels.SocketChannel

import org.apache.spark.TaskContext

import com.actian.spark_vector.vector.{ VectorConnectionProperties, ColumnMetadata }
import com.actian.spark_vector.datastream.{ VectorEndpointConf, DataStreamConnectionHeader, DataStreamConnector }
import com.actian.spark_vector.colbuffer.IntSize
import com.actian.spark_vector.Profiling
import com.actian.spark_vector.util.{ Logging, ResourceUtil }

/**
 * Entry point for unloading with spark-vector connector.
 *
 * @param vectorProps connection information to the leader node's SQL interface
 * @param tableSchema of the table as a `StructType`
 */
class DataStreamReader(readConf: VectorEndpointConf, tableColumnMetadata: Seq[ColumnMetadata])
    extends Logging with Serializable with Profiling {
  private lazy val connector = DataStreamConnector(readConf)

  /**
   * This function is executed once for each partition of [[ScanRDD]]. Will open a socket connection, read all data assigned
   * to its corresponding `partitionId` and return a row iterator (leaving the connection opened).
   */
  def read(partitionId: Int): RowReader = connector.newConnection(partitionId) { implicit socket =>
    val headerInfo = connector.readConnectionHeader()
    RowReader(tableColumnMetadata, headerInfo, DataStreamTap())
  }
  
  def touch(partitionId: Int) = connector.withConnection(partitionId)({_ => })
}

/** Contains helpers to read binary data, conforming to `Vector`'s binary protocol */
object DataStreamReader extends Logging {
  import ResourceUtil._

  // scalastyle:off magic.number
  /**
   * Read a Vector `code` from `in` and return `false` if a different
   * code was read from the `ByteBuffer`
   */
  @tailrec
  def readCode(in: ByteBuffer, code: Array[Int]): Boolean = if (in.getInt() != code(0)) {
    false
  } else {
    code.length == 1 || readCode(in, code.tail)
  }

  /** Read a variable length message's length from `in` */
  def readLength(in: ByteBuffer): Long = in.get() match {
    case x if (x & 0xFF) < 255 => x & 0xFF
    case _ => 255 + readLength(in)
  }

  /** Read a `ByteArray` from `in` */
  def readByteArray(in: ByteBuffer): Array[Byte] = {
    val len = readLength(in)
    logTrace(s"Preparing to read an array of size $len")
    val ret = Array.ofDim[Byte](len.toInt)
    in.get(ret, 0, len.toInt)
    ret
  }

  /** Read an ASCII string from `in` */
  def readString(in: ByteBuffer): String = new String(readByteArray(in), "ASCII")

  /** Read a byte buffer of length `len from `socket` */
  private def readByteBuffer(len: Int, reuseBufferOpt: Option[ByteBuffer] = None)(implicit socket: SocketChannel): ByteBuffer = closeResourceOnFailure(socket) {
    val buffer = reuseBufferOpt.getOrElse(ByteBuffer.allocateDirect(len))
    logTrace(s"${if (!reuseBufferOpt.isEmpty) "Reusing" else "Creating a new"} byte buffer of size ${buffer.capacity}")
    var i = 0
    buffer.clear()
    buffer.limit(len)
    while (i < len && socket.isConnected) {
      val j = socket.read(buffer)
      if (j <= 0) throw new Exception(
        s"Connection to Vector end point has been closed or amount of data communicated does not match the message length")
      i += j
    }
    buffer.flip()
    buffer
  }

  /** Read a byte buffer and its length integer from `socket` */
  private def readByteBufferWithLength(reuseBufferOpt: Option[ByteBuffer] = None)(implicit socket: SocketChannel): ByteBuffer = {
    val len = readByteBuffer(IntSize, reuseBufferOpt).getInt()
    logTrace(s"Reading a byte stream of size ${len - IntSize}")
    readByteBuffer(len - IntSize, reuseBufferOpt)
  }

  /** Read data from `socket`, store it in a `ByteBuffer` and execute the `code` that reads from it */
  def readWithByteBuffer[T](reuseBufferOpt: Option[ByteBuffer] = None)(code: ByteBuffer => T)(implicit socket: SocketChannel): T =
    code(readByteBufferWithLength(reuseBufferOpt))
  // scalastyle:on magic.number
}
