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

import java.io.{ ByteArrayOutputStream, DataOutputStream }
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

import scala.annotation.tailrec
import scala.concurrent.Future

import org.apache.spark.{ Logging, TaskContext }

import com.actian.spark_vector.Profiling
import com.actian.spark_vector.colbuffer.IntSize
import com.actian.spark_vector.datastream.{ VectorEndpointConf, DataStreamConnectionHeader, DataStreamConnector }
import com.actian.spark_vector.vector.{ VectorConnectionProperties, ColumnMetadata }
import com.actian.spark_vector.util.ResourceUtil

/**
 * Entry point for loading with spark-vector connector.
 *
 * @param writeConf Write configuration to be used when connecting to the `DataStream` API
 * @param table The table loaded to
 * @param tableSchema of the table as a sequence of columns metadata
 */
class DataStreamWriter[T <% Seq[Any]](writeConf: VectorEndpointConf, table: String, tableMetadataSchema: Seq[ColumnMetadata]) extends Logging with Serializable with Profiling {
  private lazy val connector = new DataStreamConnector(writeConf)

  /**
   * This function is executed once for each partition of [[InsertRDD]] and it will open a socket connection, process all data
   * assigned to its corresponding partition (`taskContext.partitionId`) and then close the connection.
   */
  def write(taskContext: TaskContext, data: Iterator[T]): Unit = connector.withConnection(taskContext.partitionId) { implicit socket =>
    val headerInfo = connector.readExternalScanConnectionHeader().validateColumnDataTypes(tableMetadataSchema)
    val rowWriter = RowWriter(tableMetadataSchema, headerInfo, DataStreamSink())
    rowWriter.write(data)
  }
}

/** Contains helpers to write binary data, conforming to `Vector`'s binary protocol */
object DataStreamWriter extends Logging {
  import ResourceUtil._

  // scalastyle:off magic.number
  /** Write the length `len` of a variable length message to `out` */
  @tailrec def writeLength(out: DataOutputStream, len: Long): Unit = len match {
    case x if x < 255 => out.writeByte(x.toInt)
    case _ =>
      out.write(255)
      writeLength(out, len - 255)
  }

  /** Write an ASCII encoded string to `out` */
  def writeString(out: DataOutputStream, s: String): Unit = writeByteArray(out, s.getBytes("ASCII"))

  /** Write a `ByteArray` `a` to `out` */
  def writeByteArray(out: DataOutputStream, a: Array[Byte]): Unit = {
    writeLength(out, a.length)
    out.write(a)
  }

  /**
   * Writes `buffer` to `socket`. Note this method assumes that the buffer is in read mode, i.e.
   * the position is at 0. To flip a `ByteBuffer` before writing, use `writeByteBuffer`
   */
  def writeByteBufferNoFlip(buffer: ByteBuffer)(implicit socket: SocketChannel): Unit = closeResourceOnFailure(socket) {
    while (buffer.hasRemaining()) socket.write(buffer)
  }

  /** Writes an integer to the socket */
  def writeInt(x: Int)(implicit socket: SocketChannel): Unit = {
    val buffer = ByteBuffer.allocateDirect(IntSize)
    buffer.putInt(x)
    writeByteBuffer(buffer)
  }

  /** Write a Vector code to `out`*/
  def writeCode(out: DataOutputStream, code: Array[Int]): Unit = code.foreach { out.writeInt(_) }

  /**
   * Write a `ByteBuffer` to `socket`. Note this method flips the byteBuffer before writing. For writing
   * a `ByteBuffer` without flipping, use `writeByteBufferNoFlip`
   */
  private def writeByteBuffer(buffer: ByteBuffer)(implicit socket: SocketChannel): Unit = {
    buffer.flip()
    writeByteBufferNoFlip(buffer)
  }

  /** Write a `ByteBuffer` preceded by its length to `socket` */
  private def writeByteBufferWithLength(buffer: ByteBuffer)(implicit socket: SocketChannel): Unit = {
    val lenByteBuffer = ByteBuffer.allocateDirect(4)
    logTrace(s"Writing a byte stream of size ${buffer.limit()}")
    lenByteBuffer.putInt(buffer.limit() + 4)
    writeByteBuffer(lenByteBuffer)
    buffer.position(buffer.limit())
    writeByteBuffer(buffer)
  }

  /** Write using a `ByteBuffer` to `socket`, exposing to the user a `DataOutputStream` */
  def writeWithByteBuffer(code: DataOutputStream => Unit)(implicit socket: SocketChannel): Unit = {
    val bos = new ByteArrayOutputStream()
    val out = new DataOutputStream(bos)
    closeResourceAfterUse(out, bos) {
      code(out)
      out.flush()
      writeByteBufferWithLength(ByteBuffer.wrap(bos.toByteArray()))
    }
  }
  // scalastyle:on magic.number
}
