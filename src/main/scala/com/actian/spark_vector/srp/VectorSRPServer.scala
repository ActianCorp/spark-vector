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
package com.actian.spark_vector.srp

import org.apache.spark.Logging
import java.nio.channels.SocketChannel
import com.actian.spark_vector.datastream.reader.DataStreamReader._
import com.actian.spark_vector.datastream.writer.DataStreamWriter._
import com.actian.spark_vector.vector.{ ErrorCodes, VectorException }

/**
 * Server for SRP authentication as implemented in Vector.
 *
 * @note Before use, user credentials need to be saved by the server, i.e. calling the `saveUserCredentials` method
 */
class VectorSRPServer extends SRPServer with Logging {
  import VectorSRP._
  import ErrorCodes._
  import Util._

  private val userCreds = collection.mutable.Map.empty[String, (Array[Byte], Array[Byte])]

  override def N: BigInt = VectorSRP.vectorN

  override def g: BigInt = VectorSRP.vectorG

  override def save(userName: String, s: Array[Byte], v: Array[Byte]): Unit = userCreds += userName -> (s, v)

  override def findSV(userName: String): Option[Tuple2[Array[Byte], Array[Byte]]] = userCreds.get(userName)

  def authenticate(implicit socket: SocketChannel) = {
    val (username, aVal) = readWithByteBuffer() { in =>
      if (!readCode(in, authCode)) throw new VectorException(AuthError, "Authentication failed: didn't receive auth code on authentication")
      val I = readString(in)
      val Aval = readByteArray(in)
      (I, Aval)
    }
    logTrace(s"Received username $username and A=${aVal.toHexString}")
    getSessionWithClientParameters(username, aVal) match {
      case Some((sesVal, kVal, s, bVal)) => {
        logTrace(s"S = ${sesVal.toHexString}, K = ${kVal.toHexString}, s = ${s.toHexString}, B = ${bVal.toHexString}")
        writeWithByteBuffer { out =>
          writeCode(out, sBCode)
          writeString(out, s.toHexString)
          writeByteArray(out, bVal)
        }
        val clientM = readWithByteBuffer() { in =>
          if (!readCode(in, MCode)) throw new VectorException(AuthError, "Authentication failed: unable to read code before verification of client M key")
          readByteArray(in)
        }
        logTrace(s"Received clientM=${clientM.toHexString}, server version is ${VectorSRPClient.M(username, s, aVal, bVal, kVal).toHexString}")
        if (!clientM.sameElements(VectorSRPClient.M(username, s, aVal, bVal, kVal)))
          throw new VectorException(AuthError, "Authentication failed: client M differs from server M")
        writeWithByteBuffer { out =>
          writeCode(out, serverMCode)
          writeByteArray(out, M(aVal, clientM, kVal))
        }
      }
      case None => throw new VectorException(AuthError, s"Authentication failed: username $username is not recognized")
    }
  }
}
