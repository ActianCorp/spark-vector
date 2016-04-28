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
package com.actian.spark_vector.datastream

import java.net.InetSocketAddress
import java.nio.channels.SocketChannel
import java.nio.ByteBuffer

import org.apache.spark.Logging

import com.actian.spark_vector.util.ResourceUtil.{ closeResourceOnFailure, closeResourceAfterUse }
import com.actian.spark_vector.datastream.reader.DataStreamReader
import com.actian.spark_vector.vector.ColumnMetadata
import com.actian.spark_vector.srp.VectorSRPClient
import com.actian.spark_vector.colbuffer.IntSize

/**
 * Container for the datastream connection header info.
 * @note we keep just a part of this info, the rest of it such as column names and their logical/physical types
 * is not stored in this container since we get it earlier from a JDBC query.
 */
private[datastream] case class DataStreamConnectionHeader(header: ByteBuffer) {
  // scalastyle:off magic.number
  private def uByte(value: Byte) = if (value < 0) value + 256 else value

  val statusCode = header.getInt()

  private val numCols = header.getInt()

  val vectorSize = header.getInt()

  lazy val isNullableCol = {
    val nullCol = Array.fill[Boolean](numCols)(false)
    var i = 0
    while (i < numCols) {
      var sizeValue = uByte(header.get())
      var colNameSize = sizeValue
      while (sizeValue == 255) {
        sizeValue = uByte(header.get())
        colNameSize += sizeValue
      }
      nullCol(i) = colNameSize == 0
      while (colNameSize > 0) {
        header.get()
        colNameSize -= 1
      }
      i += 1
    }
    nullCol
  }
  // scalastyle:on magic.number

  require(statusCode >= 0, "Invalid status code: error reading data.")

  def validateColumnDataTypes(tableMetadataSchema: Seq[ColumnMetadata]): DataStreamConnectionHeader = {
    // TODO: header sanity check, throwing some exceptions in case of inconsistencies
    // TODO: support const value types as well
    this
  }
}

/**
 * Class containing methods to open connections to Vector's `DataStream` API
 *
 * @param writeConf `DataStream` information containing at least the number of connections expected, the names and
 * ports of the hosts where they are expected and authentication information
 */
private[datastream] class DataStreamConnector(conf: VectorEndpointConf) extends Logging with Serializable {
  import DataStreamReader._

  private def openSocketChannel(idx: Int): SocketChannel = {
    val host: VectorEndpoint = conf.vectorEndpoints(idx)
    logDebug(s"Opening a socket to $host")
    implicit val socket = SocketChannel.open()
    socket.connect(new InetSocketAddress(host.host, host.port))
    val srpClient = new VectorSRPClient(host.username, host.password)
    closeResourceOnFailure(socket) { srpClient.authenticate }
    socket
  }

  /** Open a connection to Vector and execute the code specified by `op` */
  def withConnection[T](idx: Int)(op: SocketChannel => T): T = {
    val socket = openSocketChannel(idx)
    closeResourceAfterUse(socket) { op(socket) }
  }

  /** Open a connection to Vector, execute the code specified by `op` and leave the socket open */
  def newConnection[T](idx: Int)(op: SocketChannel => T): T = {
    val socket = openSocketChannel(idx)
    closeResourceOnFailure(socket) { op(socket) }
  }

  def readExternalScanConnectionHeader()(implicit socket: SocketChannel): DataStreamConnectionHeader = {
    readWithByteBuffer() { in => } // get_table_info column definition header
    readWithByteBuffer() { in => } // actual data of the get_table_info
    readWithByteBuffer() { in => } // end of get_table_info query
    readWithByteBuffer() { in => DataStreamConnectionHeader(in) } // query response for data loading
  }

  def readExternalInsertConnectionHeader()(implicit socket: SocketChannel): DataStreamConnectionHeader = readWithByteBuffer() {
    in => DataStreamConnectionHeader(in)
  }
}

private[datastream] object DataStreamConnector {
  // @note this is the binary data header's size (NOT the connection header's size)
  final val DataHeaderSize =  IntSize /* messageLength */ + IntSize /* binaryDataCode */ + IntSize /* numTuples */

  def apply(conf: VectorEndpointConf): DataStreamConnector = new DataStreamConnector(conf)
}