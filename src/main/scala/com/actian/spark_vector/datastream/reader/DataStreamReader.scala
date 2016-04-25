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

import org.apache.spark.{ Logging, TaskContext }

import com.actian.spark_vector.vector.{ VectorConnectionProperties, ColumnMetadata }
import com.actian.spark_vector.datastream.{ DataStreamClient, DataStreamConnectionHeader, DataStreamConnector }
import com.actian.spark_vector.colbuffer.IntSize
import com.actian.spark_vector.Profiling
import com.actian.spark_vector.util.ResourceUtil

/**
 * Entry point for unloading with spark-vector connector.
 *
 * @param vectorProps connection information to the leader node's SQL interface
 * @param table The table to unload from
 * @param tableSchema of the table as a `StructType`
 */
class DataStreamReader(vectorProps: VectorConnectionProperties, table: String, tableMetadataSchema: Seq[ColumnMetadata])
  extends Logging with Serializable with Profiling {
  /**
   * A client to connect to the `Vector`'s SQL interface through JDBC. Currently used to
   * obtain `DataStream` connection information (# of streams, hosts, roles, etc.) and to submit
   * the unload query.
   *
   * @note Available only on the driver.
   */
  @transient val client = DataStreamClient(vectorProps, table)
  /** Write configuration to be used when connecting to the `DataStream` API */
  lazy val readConf = {
    client.prepareUnloadDataStreams
    client.getVectorEndpointConf
  }
  private lazy val connector = DataStreamConnector(readConf)

  /**
   * This function is executed once for each partition of [[ScanRDD]]. Will open a socket connection, read all data assigned
   * to its corresponding partition (`taskContext.partitionId`) and return a row iterator (leaving the connection opened).
   */
  def read(taskContext: TaskContext): RowReader = connector.newConnection(taskContext.partitionId)(
    implicit socket => {
      val headerInfo = connector.readExternalInsertConnectionHeader().validateColumnInfo(tableMetadataSchema)
      RowReader(tableMetadataSchema, headerInfo, DataStreamTap())
    }
  )

  /**
   * Initiate the unload, i.e. submit the SQL query to start unloading to an external source.
   *
   * @note should only be called on the driver.
   */
  def initiateUnload(preparedSelect: String, whereParams: Seq[Any]): Future[Int] = client.startUnload(preparedSelect, whereParams)

  /**
   * Commit the last transaction started by this `DataStreamReader`'s client.
   *
   * @note should only be called on the driver.
   */
  def commit: Unit = client.commit
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
  def readByteBuffer(len: Int, reuseBufferOpt: Option[ByteBuffer] = None)(implicit socket: SocketChannel): ByteBuffer = {
    val buffer = reuseBufferOpt.getOrElse({ByteBuffer.allocate(len)})
    logDebug(s"${if (!reuseBufferOpt.isEmpty) "Reusing" else "Creating a new"} byte buffer of size ${buffer.capacity}")
    var i = 0
    buffer.clear()
    buffer.limit(len)
    while (i < len) {
      val j = socket.read(buffer)
      if (j <= 0) throw new Exception(
        s"Connection to Vector end point has been closed or amount of data communicated does not match the message length")
      i += j
    }
    buffer.flip()
    buffer
  }

  /** Read a byte buffer and its length integer from `socket` */
  def readByteBufferWithLength(reuseBufferOpt: Option[ByteBuffer] = None)(implicit socket: SocketChannel): ByteBuffer = {
    val len = readByteBuffer(IntSize, reuseBufferOpt).getInt()
    logTrace(s"Reading a byte stream of size ${len - IntSize}")
    readByteBuffer(len - IntSize, reuseBufferOpt)
  }

  /** Read data from `socket`, store it in a `ByteBuffer` and execute the `code` that reads from it */
  def readWithByteBuffer[T](reuseBufferOpt: Option[ByteBuffer] = None)(code: ByteBuffer => T)(implicit socket: SocketChannel): T =
    closeResourceOnFailure(socket) { code(readByteBufferWithLength(reuseBufferOpt))  }
  // scalastyle:on magic.number
}
