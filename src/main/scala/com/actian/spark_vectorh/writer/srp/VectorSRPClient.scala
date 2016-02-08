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
package com.actian.spark_vectorh.writer.srp

import java.nio.channels.SocketChannel
import scala.BigInt
import scala.annotation.tailrec
import org.apache.spark.Logging
import com.actian.spark_vectorh.writer.{ DataStreamReader, DataStreamWriter }
import com.actian.spark_vectorh.vector.ErrorCodes._
import com.actian.spark_vectorh.vector.VectorException

/** Performs authentication to `Vector(H)` with SRP (Secure Remote Password) protocol */
// scalastyle:off magic.number
class VectorSRPClient(username: String, password: String) extends ClientSRPParameter with Logging {
  import VectorSRPClient._
  import DataStreamReader._
  import DataStreamWriter._
  import Util._

  /* The following are taken from RFC 5054, 8192-bit group. Override to match the one on the `Vector(H)` side */
  override def N: BigInt = BigInt("""
  |FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E08
  |8A67CC74020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B
  |302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C42E9
  |A637ED6B0BFF5CB6F406B7EDEE386BFB5A899FA5AE9F24117C4B1FE6
  |49286651ECE45B3DC2007CB8A163BF0598DA48361C55D39A69163FA8
  |FD24CF5F83655D23DCA3AD961C62F356208552BB9ED529077096966D
  |670C354E4ABC9804F1746C08CA18217C32905E462E36CE3BE39E772C
  |180E86039B2783A2EC07A28FB5C55DF06F4C52C9DE2BCBF695581718
  |3995497CEA956AE515D2261898FA051015728E5A8AAAC42DAD33170D
  |04507A33A85521ABDF1CBA64ECFB850458DBEF0A8AEA71575D060C7D
  |B3970F85A6E1E4C7ABF5AE8CDB0933D71E8C94E04A25619DCEE3D226
  |1AD2EE6BF12FFA06D98A0864D87602733EC86A64521F2B18177B200C
  |BBE117577A615D6C770988C0BAD946E208E24FA074E5AB3143DB5BFC
  |E0FD108E4B82D120A92108011A723C12A787E6D788719A10BDBA5B26
  |99C327186AF4E23C1A946834B6150BDA2583E9CA2AD44CE8DBBBC2DB
  |04DE8EF92E8EFC141FBECAA6287C59474E6BC05D99B2964FA090C3A2
  |233BA186515BE7ED1F612970CEE2D7AFB81BDD762170481CD0069127
  |D5B05AA993B4EA988D8FDDC186FFB7DC90A6C08F4DF435C934028492
  |36C3FAB4D27C7026C1D4DCB2602646DEC9751E763DBA37BDF8FF9406
  |AD9E530EE5DB382F413001AEB06A53ED9027D831179727B0865A8918
  |DA3EDBEBCF9B14ED44CE6CBACED4BB1BDB7F1447E6CC254B33205151
  |2BD7AF426FB8F401378CD2BF5983CA01C64B92ECF032EA15D1721D03
  |F482D7CE6E74FEF6D55E702F46980C82B5A84031900B1C9E59E7C97F
  |BEC7E8F323A97A7E36CC88BE0F1D45B7FF585AC54BD407B22B4154AA
  |CC8F6D7EBF48E1D814CC5ED20F8037E0A79715EEF29BE32806A1D58B
  |B7C5DA76F550AA3D8A1FBFF0EB19CCB1A313D55CDA56C9EC2EF29632
  |387FE8D76E3C0468043E8F663F4860EE12BF2D5B0B7474D6E694F91E
  |6DBE115974A3926F12FEE5E438777CB6A932DF8CD8BEC4D073B931BA
  |3BC832B68D9DD300741FA7BF8AFC47ED2576F6936BA424663AAB639C
  |5AE4F5683423B4742BF1C978238F16CBE39D652DE3FDB8BEFC848AD9
  |22222E04A4037C0713EB57A81A23F0C73473FC646CEA306B4BCBC886
  |2F8385DDFA9D4B7FA2C087E879683303ED5BDD3A062B3CF5B3A278A6
  |6D2A13F83F44F82DDF310EE074AB6A364597E899A0255DC164F31CC5
  |0846851DF9AB48195DED7EA1B1D510BD7EE74D73FAF36BC31ECFA268
  |359046F4EB879F924009438B481C6CD7889A002ED5EE382BC9190DA6
  |FC026E479558E4475677E9AA9E3050E2765694DFC81F56E880B96E71
  |60C980DD98EDD3DFFFFFFFFFFFFFFFFF""".stripMargin.replaceAll("\n", ""), 16)

  /** Override to match the one on the `Vector(H)` side */
  override def g: BigInt = BigInt("13", 16)

  def x(I: Array[Byte], s: Array[Byte], password: Array[Byte]): Array[Byte] =
    H(s, H(I ++ (":".getBytes("ASCII")), password))

  def M(s: Array[Byte], A: Array[Byte], B: Array[Byte], K: Array[Byte]): Array[Byte] = {
    val HN = H(Util.removeBitSign(N.toByteArray))
    val Hg = H(g.toByteArray)
    val HI = H(username.getBytes("ASCII"))
    val HNxorHg = HN.zip(Hg).map { case (left, right) => (left ^ right).toByte }
    H(HNxorHg ++ HI ++ s ++ A ++ B ++ K)
  }

  /** Authenticate by sending the sequence of messages exchanged during SRP through `socket` */
  def authenticate(implicit socket: SocketChannel): Unit = {
    val a = super.a
    val A = super.A(a)
    writeWithByteBuffer { out =>
      writeCode(out, authCode)
      writeString(out, username)
      writeByteArray(out, A)
    }
    val (s, b) =
      readWithByteBuffer[(Array[Byte], Array[Byte])] { in =>
        if (!readCode(in, sBCode)) {
          throw new VectorException(AuthError, "Unable to read Ok code after exchanging username and A")
        }
        (Util.removeBitSign(BigInt(readString(in), 16).toByteArray), readByteArray(in))
      }
    val B = b
    val u = super.u(A, B)
    val x = this.x(username.getBytes("ASCII"), s, password.getBytes("ASCII"))
    val S = super.S(BigInt(addBitSign(x)), BigInt(addBitSign(B)), BigInt(a), BigInt(addBitSign(u)))
    val K = H(S)
    val clientM = M(s, A, B, K)
    writeWithByteBuffer { out =>
      writeCode(out, MCode)
      writeByteArray(out, clientM)
    }
    val serverM = readWithByteBuffer[Array[Byte]] { in =>
      if (!readCode(in, serverMCode)) {
        throw new VectorException(AuthError, "Unable to read code before verification of server M key")
      }
      readByteArray(in)
    }
    if (!H(A ++ clientM ++ K).sameElements(serverM)) {
      throw new VectorException(AuthError, "M and serverM differ")
    }
  }
}

/** Contains some `Vector(H)` codes to be used while authenticating and what algorithm to use */
object VectorSRPClient {
  private val authCode = Array(4, 1, 0) /* srp */
  private val sBCode = Array(4, 1, 1)
  private val MCode = Array(4, 1, 2)
  private val serverMCode = Array(4, 1, 3)
  private val shaAlgorithm = "SHA-512"
}
// scalastyle:on magic.number
