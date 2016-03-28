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

import java.net.InetSocketAddress
import java.nio.channels.SocketChannel

import org.apache.spark.Logging

import com.actian.spark_vector.util.ResourceUtil.closeResourceAfterUse
import com.actian.spark_vector.writer.srp.VectorSRPClient
import com.actian.spark_vector.reader.DataStreamReader
import com.actian.spark_vector.vector.VectorConnectionHeader

/**
 * Class containing methods to open connections to Vector's `DataStream` API
 *
 * @param writeConf `DataStream` information containing at least the number of connections expected, the names and
 * ports of the hosts where they are expected and authentication information
 */
class DataStreamConnector(writeConf: WriteConf) extends Logging with Serializable {
  import DataStreamReader._

  private def openConnection(idx: Int): SocketChannel = {
    val host: VectorEndPoint = writeConf.vectorEndPoints(idx)
    logInfo(s"Opening a socket to $host")
    implicit val socket = SocketChannel.open()
    socket.connect(new InetSocketAddress(host.host, host.port))
    val srpClient = new VectorSRPClient(host.username, host.password)
    srpClient.authenticate
    socket
  }

  /** Open a connection to Vector and execute the code specified by `op` */
  def withConnection[T](idx: Int)(op: SocketChannel => T): T = {
    val socket = openConnection(idx)
    closeResourceAfterUse(socket) { op(socket) }
  }

  def readConnectionHeader(implicit socket: SocketChannel): VectorConnectionHeader = {
    readWithByteBuffer { in => } // get_table_info column definition header
    readWithByteBuffer { in => } // actual data of the get_table_info
    readWithByteBuffer { in => } // end of get_table_info query
    readWithByteBuffer { in => // query response for data loading
      /** There is more information on datatypes and nullability before, but we ignore it since we got it earlier from a JDBC query. */
      VectorConnectionHeader(in.getInt(VectorConnectionHeader.StatusCodeIndex), in.getInt(VectorConnectionHeader.VectorSizeIndex))
    }
  }
}
