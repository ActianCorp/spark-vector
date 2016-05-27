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

import java.nio.channels.SocketChannel
import scala.BigInt
import scala.annotation.tailrec
import org.apache.spark.Logging
import com.actian.spark_vector.datastream.writer.DataStreamWriter
import com.actian.spark_vector.datastream.reader.DataStreamReader
import com.actian.spark_vector.vector.ErrorCodes._
import com.actian.spark_vector.vector.VectorException

/** Performs authentication to `Vector` with SRP (Secure Remote Password) protocol */
// scalastyle:off magic.number
object VectorSRPClient extends ClientSRPParameter with Logging {
  import DataStreamReader._
  import DataStreamWriter._
  import Util._
  import VectorSRP._

  override def N: BigInt = VectorSRP.vectorN

  /** Override to match the one on the `Vector` side */
  override def g: BigInt = VectorSRP.vectorG

  /** Authenticate by sending the sequence of messages exchanged during SRP through `socket`, acting as the client */
  def authenticate(username: String, password: String)(implicit socket: SocketChannel): Unit = {
    val a = super.a
    val A = super.A(a)
    writeWithByteBuffer { out =>
      writeCode(out, authCode)
      writeString(out, username)
      writeByteArray(out, A)
    }
    val (s, b): (Array[Byte], Array[Byte]) = readWithByteBuffer[(Array[Byte], Array[Byte])]() { in =>
      if (!readCode(in, sBCode)) {
        throw new VectorException(AuthError, "Authentication failed: unable to read Ok code after exchanging username and A")
      }
      (BigInt(readString(in), 16), readByteArray(in))
    }
    val B = b
    val u = super.u(A, B)
    val x = super.x(s, username, password)
    val S = super.S(x, B, a, u)
    val K = H(S)
    val clientM = M(username, s, A, B, K)
    writeWithByteBuffer { out =>
      writeCode(out, MCode)
      writeByteArray(out, clientM)
    }
    val serverM = readWithByteBuffer[Array[Byte]]() { in =>
      if (!readCode(in, serverMCode)) {
        throw new VectorException(AuthError, "Authentication failed: unable to read code before verification of server M key")
      }
      readByteArray(in)
    }
    if (!H(A ++ clientM ++ K).sameElements(serverM)) {
      throw new VectorException(AuthError, "Authentication failed: M and serverM differ")
    }
  }
}
// scalastyle:on magic.number
