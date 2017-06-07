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
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

import com.actian.spark_vector.colbuffer.IntSize
import com.actian.spark_vector.datastream.reader.DataStreamReader
import com.actian.spark_vector.srp.VectorSRPClient
import com.actian.spark_vector.util.Logging
import com.actian.spark_vector.util.ResourceUtil.{ closeResourceAfterUse, closeResourceOnFailure }
import com.actian.spark_vector.vector.{ ColumnMetadata, VectorNet }

/**
 * Container for the datastream connection header info.
 *
 * @note We keep just a part of this info, the rest of it such as column names and their logical/physical types
 * is not stored in this container since we get it earlier from a JDBC query.
 * @note From `colInfo` only use the `nullable` field.
 */
private[datastream] case class DataStreamConnectionHeader(statusCode: Int,
    numCols: Int,
    vectorSize: Int,
    colInfo: Seq[ColumnMetadata]) extends Serializable { // For now holding col info only for name and nullability
  /** TODO: Sanity check for column data types, throwing some exceptions in case of inconsistencies */
  require(statusCode >= 0, "Invalid status code (possible errors during connection).")
}

private object DataStreamConnectionHeader extends Serializable {
  import DataStreamReader._

  private final val StatusCodeIndex = 0
  private final val NumColsIndex = 4
  private final val VectorSizeIndex = 8
  private final val ColInfoIndex = 12

  def apply(header: ByteBuffer): DataStreamConnectionHeader = {
    val statusCode = header.getInt(StatusCodeIndex)
    val numCols = header.getInt(NumColsIndex)
    val vectorSize = header.getInt(VectorSizeIndex)
    val colInfo = {
      header.position(ColInfoIndex)
      val headerColInfo = (
        Array.fill[String](numCols) { readString(header) } zip
        Array.fill[(Boolean, String, String)](numCols) {
          (header.getInt() == 1, readString(header), readString(header))
        })
      headerColInfo.foldLeft((Seq[ColumnMetadata](), false)) {
        case ((seq, prevWasMarker), (name, (_, _, _))) =>
          /** TODO: infer/parse column type info, obtain scale and precision info too */
          /** WARN: column name needs escaping in Vector */
          if (name.isEmpty) {
            (seq, true) // mark it as a marker, w/o adding column metadata
          } else {
            (seq :+ ColumnMetadata(name, "", prevWasMarker, 0, 0), false)
          }
      }._1
    }
    new DataStreamConnectionHeader(statusCode, numCols, vectorSize, colInfo)
  }
}

/**
 * Class containing methods to open connections to Vector's `DataStream` API
 *
 * @param writeConf `DataStream` information containing at least the number of connections expected, the names and
 * ports of the hosts where they are expected and authentication information
 */
private[datastream] class DataStreamConnector(conf: VectorEndpointConf) extends Logging with Serializable {
  import DataStreamConnector._
  import DataStreamReader._

  private def openSocketChannel(idx: Int): SocketChannel = {
    val host: VectorEndpoint = conf.vectorEndpoints(idx)
    logDebug(s"Opening a socket to $host")
    implicit val socket = SocketChannel.open()
    socket.connect(new InetSocketAddress(host.host, host.port))
    closeResourceOnFailure(socket) {
      VectorNet.clientCheckVersion()
      VectorSRPClient.authenticate(host.username, host.password)
      VectorNet.writeClientType(DataStreamClientType)
    }
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

  def readConnectionHeader()(implicit socket: SocketChannel): DataStreamConnectionHeader =
    readWithByteBuffer() { in => DataStreamConnectionHeader(in) } // query response for data loading/unloading
}

private[datastream] object DataStreamConnector {
  /** @note This is the binary data header's size (NOT the connection header's size) */
  final val DataHeaderSize = IntSize /* messageLength */ + IntSize /* binaryDataCode */ + IntSize /* numTuples */
  private final val DataStreamClientType = 2 /* CLIENTTYPE_DATASTREAM */

  def apply(conf: VectorEndpointConf): DataStreamConnector = new DataStreamConnector(conf)
}
