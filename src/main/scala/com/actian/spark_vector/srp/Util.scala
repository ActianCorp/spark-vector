/*
 * All code is released under MIT license. Contact us on website for
 * any kind of support.
 * Author: Shreyas Purohit
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is furnished
 * to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 *
 * In addition, the following condition applies:
 * Any other license of the libraries either javascript or java/scala used still
 * applies and must be used according to its licensing information.
 */
package com.actian.spark_vector.srp

import java.security._
import scala.language.implicitConversions

import com.actian.spark_vector.util.Logging

// scalastyle:off
/**
 * The SRP 6a version as defined at http://srp.stanford.edu/.
 * Design: http://srp.stanford.edu/design.html
 *
 * The following is a description of SRP-6 and 6a, the latest versions of SRP:
 * N    A large safe prime (N = 2q+1, where q is prime)
 * All arithmetic is done modulo N.
 * g    A generator modulo N
 * k    Multiplier parameter (k = H(N, g) in SRP-6a, k = 3 for legacy SRP-6)
 * s    User's salt
 * I    Username
 * p    Cleartext Password
 * H()  One-way hash function
 * ^    (Modular) Exponentiation
 * u    Random scrambling parameter
 * a,b  Secret ephemeral values
 * A,B  Public ephemeral values
 * x    Private key (derived from p and s)
 * v    Password verifier
 * The host stores passwords using the following formula:
 * x = H(s, p)               (s is chosen randomly)
 * v = g^x                   (computes password verifier)
 * The host then keeps {I, s, v} in its password database. The authentication protocol itself goes as follows:
 * User -> Host:  I, A = g^a                  (identifies self, a = random number)
 * Host -> User:  s, B = kv + g^b             (sends salt, b = random number)
 * Both:  u = H(A, B)
 * User:  x = H(s, p)                 (user enters password)
 * User:  S = (B - kg^x) ^ (a + ux)   (computes session key)
 * User:  K = H(S)
 * Host:  S = (Av^u) ^ b              (computes session key)
 * Host:  K = H(S)
 */
object Util {
  //H() is a hash function; e.g., SHA-512.
  /**
   * @param data The byte array that needs to be Hashed.
   * @param salt The byte array that is used as a salt for the hash. Default: Empty array
   * @param algorithm The algorithm to be used for Hashing- SHA-1, SHA-256, SHA-512. Default: SHA-512
   * @param iteration The number of iteration to use for Hashing. Default: 3
   * @return hash The byte array that is hashed using algorithm, iteration, data and salt
   */
  def H(data: Array[Byte], salt: Array[Byte] = Array(), algorithm: String = "SHA-512", iteration: Int = 1): Array[Byte] = {
    val temp: Array[Byte] = data ++ salt
    val digest = MessageDigest.getInstance(algorithm);
    var input: Array[Byte] = digest.digest(temp);
    Range(1, iteration).foreach(n => {
      input = digest.digest(input)
    })
    input
  }

  class HexVal(bytes: Array[Byte]) {
    def toHexString = bytes.map("%02x" format _).mkString
  }

  implicit def hexBytesWrapper(bytes: Array[Byte]) = new HexVal(bytes)

  private def removeBitSign(x: Array[Byte]) =
    x(0) match {
      case 0 => x.tail
      case _ => x
    }

  private def addBitSign(x: Array[Byte]) =
    x(0) & 128 match {
      case 0 => x
      case _ => Array(0.toByte) ++ x
    }

  implicit def positiveBigInt(v: Array[Byte]): BigInt = BigInt(addBitSign(v))

  implicit def bigIntToByteArrayWithoutSign(v: BigInt): Array[Byte] = removeBitSign(v.toByteArray)
}

/**
 * The base trait that defines N, g, k, u SRP parameters and provides
 * functionality to generate random bytes, and g ^ x (mod N). The SRP parameters
 * are calculated using SRp version 6a.
 */
trait SRPParameter extends Logging {
  import Util._
  val sr = new SecureRandom

  //q and N = 2q + 1 are chosen such that both are prime (N is a safe prime and q is a Sophie Germain prime). N must be large enough so that computing discrete logarithms modulo N is infeasible.
  def N = BigInt("d4c7f8a2b32c11b8fba9581ec4ba4f1b04215642ef7355e37c0fc0443ef756ea2c6b8eeb755a1c723027663caa265ef785b8ff6a9b35227a52d86633dbdfca43", 16)
  def q = (N - 1) / 2

  //g is a generator of the multiplicative group
  def g = BigInt("2", 16)

  //k is a parameter derived by both sides; for example, k = H(N, g).
  def k = H(N, g)

  //u is calculated by both client and server
  def u(A: Array[Byte], B: Array[Byte]) = H(A, B)

  // g ^ x (mod N)
  def gPowXModN(x: Array[Byte]) = g modPow (x, N)

  //Generates 32 random byte array
  def gen32RandomBytes = {
    val bytes: Array[Byte] = new Array(32)
    sr.nextBytes(bytes)
    bytes
  }

  def x(s: Array[Byte], username: String, password: String): Array[Byte] = {
    val ret = H(s, H(s"$username:$password".getBytes("ASCII")))
    logTrace(s"x = ${ret.toHexString}")
    ret
  }
}

/**
 *  The trait that defines the calculations required on the
 *  client side- S, a, x and A
 */
trait ClientSRPParameter extends SRPParameter {
  import Util._

  //S= (B - kg^x) ^ (a + ux)   (mod N)
  def S(x: BigInt, B: BigInt, a: BigInt, u: BigInt): Array[Byte] = {
    val bx = g.modPow(x, N)
    val btmp = ((B + N * k) - (k * bx)).mod(N)
    (btmp modPow (a + (u * x), N)).mod(N)
  }

  def a: Array[Byte] = gen32RandomBytes

  def A(abytes: Array[Byte]): Array[Byte] = gPowXModN(abytes)

  //M1 = H([H(N) xor H(g)], H(I), s, A, B, K)
  def M(username: String, s: Array[Byte], A: Array[Byte], B: Array[Byte], K: Array[Byte]): Array[Byte] = {
    logTrace(s"N = ${bigIntToByteArrayWithoutSign(N).toHexString}")
    val HN = H(N)
    logTrace(s"HN = ${HN.toHexString}")
    val Hg = H(g)
    logTrace(s"Hg = ${Hg.toHexString}")
    val HI = H(username.getBytes("ASCII"))
    logTrace(s"HI = ${HI.toHexString}")
    val HNxorHg = HN.zip(Hg).map { case (left, right) => (left ^ right).toByte }
    logTrace(s"HNxorHg = ${HNxorHg.toHexString}")
    val ret = H(HNxorHg ++ HI ++ s ++ A ++ B ++ K)
    logTrace(s"clientM = ${ret.toHexString}")
    ret
  }
}

/**
 *  The trait that defines the calculations required on the
 *  server side- S, x, v and B
 */
trait ServerSRPParameter extends SRPParameter {
  import Util._
  //  The host stores x and the salt(s) using the following formula:
  //  x = H(s, p)               (s is chosen randomly)
  /**
   * @return Tuple2 _1: salt, _2: x
   */
  def x(username: String, password: String): (Array[Byte], Array[Byte]) = {
    val bytes: Array[Byte] = gen32RandomBytes
    (bytes, x(bytes, username, password))
  }

  //The host stores v using:
  //v = g^x (mod N)                  (computes password verifier)
  def v(x: Array[Byte]): Array[Byte] = gPowXModN(x)

  def b = gen32RandomBytes

  //B = kv + g^b (mod N)
  def B(vVal: Array[Byte], bVal: Array[Byte]): Array[Byte] =
    (k * vVal + gPowXModN(bVal)).mod(N)

  //S = (Av^u) ^ b (mod N)
  def S(A: Array[Byte], vVal: Array[Byte], u: Array[Byte], bVal: Array[Byte]): Array[Byte] =
    (vVal.modPow(u, N) * A).mod(N).modPow(bVal, N)

  //M2 = H(A, M1, K)
  def M(A: Array[Byte], clientM: Array[Byte], K: Array[Byte]): Array[Byte] = {
    val ret = Util.H(A ++ clientM ++ K)
    logTrace(s"serverM = ${ret.toHexString}")
    ret
  }
}

/**
 * The trait that needs to be extended to get SRP server side
 * computations.
 *
 */
trait SRPServer extends ServerSRPParameter {
  import Util._

  /**
   * Saves the calculated user credentials- userName, s, v
   *  @param username user name
   *  @param password The password to be used to save user credentials
   *  @return Tuple3[Array[Byte],Array[Byte],Array[Byte]] s,x and v
   */
  def saveUserCredentials(userName: String, password: String) = {
    val (sVal, xVal) = x(userName, password)
    val vVal = v(xVal)
    save(userName, sVal, vVal)
    (sVal, xVal, vVal)
  }

  /**
   * Given userName and A received from the client, this method generates
   * the expected current session ID when generated parameters s, B are returned to
   * the client.
   *
   * @param userName The username of the client
   * @param AVal The parameter A sent from the client.
   * @return Option[Tuple4[String,String,String,String]] An Option Tuple of sessionId, Hash(sessionId), s, B
   *
   */
  def getSessionWithClientParameters(userName: String, A: Array[Byte]) = {

    val sv = findSV(userName)
    if (sv.isEmpty) {
      None
    } else {
      val (s, v) = sv.get
      val bVal = b
      logTrace(s"b = ${b.toHexString}")
      val BVal = B(v, bVal)
      logTrace(s"B = ${BVal.toHexString}")
      val uVal = u(A, BVal)
      logTrace(s"u = ${uVal.toHexString}")
      val sessionId = S(A, v, uVal, bVal)
      logTrace(s"S = ${sessionId.toHexString}")
      Some((sessionId, H(sessionId), s, BVal))
    }
  }

  /**
   * Saves the user credentials provided
   */
  def save(userName: String, s: Array[Byte], v: Array[Byte]): Unit

  /**
   * Finder method to get the s and v for the given user with userName
   */
  def findSV(userName: String): Option[Tuple2[Array[Byte], Array[Byte]]]
}
// scalastyle:off
