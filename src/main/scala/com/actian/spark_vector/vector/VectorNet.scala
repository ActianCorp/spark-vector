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
package com.actian.spark_vector.vector

import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

import com.actian.spark_vector.datastream.reader.DataStreamReader.readWithByteBuffer
import com.actian.spark_vector.datastream.writer.DataStreamWriter.writeWithByteBuffer

/**
 * Helper functions for communicating with Vector.
 *
 * @note These functions follow the Vector network communication protocol.
 */
object VectorNet {
  final val CompatibleVectorVersion = 1 /* X100NET_V5_0_0 */
  final val VersionPacketType = 7 /* X100CPT_VERSION */
  final val ClientPacketType = 8 /* X100CPT_CLIENTTYPE */
  final val SuccessPacketType = 0 /* X100CPT_SUCCESS */

  /** Reads a packet type from `in` and verifies that it equals `expectedPacketType`, throwing an exception otherwise */
  def checkPacketType(expectedPacketType: Int, in: ByteBuffer): Unit = if (in.getInt != expectedPacketType) {
    throw new VectorException(ErrorCodes.CommunicationError, s"Invalid packet type: expected($expectedPacketType)")
  }

  /**
   * Performs protocol version exchange with Vector, acting as a client.
   *
   * @note If the version received from the server is not compatible with what is expected(i.e. `version`), it will
   * throw an exception
   */
  def clientCheckVersion(version: Int = CompatibleVectorVersion)(implicit socket: SocketChannel): Unit = {
    writeWithByteBuffer { _.writeInt(VersionPacketType) }
    readWithByteBuffer() { in =>
      checkPacketType(VersionPacketType, in)
      val serverVersion = in.getInt
      if (serverVersion != version) {
        throw new VectorException(ErrorCodes.CommunicationError, s"Server version($serverVersion) is not compatible with client version(${version})")
      }
    }
  }

  /** Write a connection client type message through `socket` */
  def writeClientType(clientType: Int)(implicit socket: SocketChannel): Unit = {
    writeWithByteBuffer { out =>
      out.writeInt(ClientPacketType)
      out.writeInt(clientType)
    }
    readWithByteBuffer() { checkPacketType(SuccessPacketType, _) }
  }

  /** Performs protocol version exchange with Vector, acting as a server. */
  def serverCheckVersion(version: Int)(implicit socket: SocketChannel): Unit = {
    readWithByteBuffer() { checkPacketType(VersionPacketType, _) }
    writeWithByteBuffer { out =>
      out.writeInt(VersionPacketType)
      out.writeInt(version)
    }
  }

  /** Reads a connection client type from the socket, throwing an exception if it does not match `expectedClientType` */
  def readClientType(expectedClientType: Int)(implicit socket: SocketChannel): Unit = {
    readWithByteBuffer() { in =>
      checkPacketType(ClientPacketType, in)
      val clientType = in.getInt
      if (clientType != expectedClientType) {
        throw new VectorException(ErrorCodes.CommunicationError, s"Unexpected client type($clientType). Expected($expectedClientType)")
      }
    }
    writeWithByteBuffer { _.writeInt(SuccessPacketType) }
  }
}
