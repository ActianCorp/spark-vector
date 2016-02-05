package com.actian.spark_vectorh.writer.srp

import java.security._
import scala.language.implicitConversions

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

  def removeBitSign(x: Array[Byte]) =
    x(0) match {
      case 0 => x.tail
      case _ => x
    }

  def addBitSign(x: Array[Byte]) =
    x(0) & 128 match {
      case 0 => x
      case _ => Array(0.toByte) ++ x
    }
}

/**
 * The base trait that defines N, g, k, u SRP parameters and provides
 * functionality to generate random bytes, and g ^ x (mod N). The SRP parameters
 * are calculated using SRp version 6a.
 */
trait SRPParameter {
  import Util._
  val sr = new SecureRandom

  //q and N = 2q + 1 are chosen such that both are prime (N is a safe prime and q is a Sophie Germain prime). N must be large enough so that computing discrete logarithms modulo N is infeasible.
  def N = BigInt("d4c7f8a2b32c11b8fba9581ec4ba4f1b04215642ef7355e37c0fc0443ef756ea2c6b8eeb755a1c723027663caa265ef785b8ff6a9b35227a52d86633dbdfca43", 16)
  def q = (N - 1) / 2

  //g is a generator of the multiplicative group
  def g = BigInt("2", 16)

  //k is a parameter derived by both sides; for example, k = H(N, g).
  def k = H(removeBitSign(N.toByteArray), removeBitSign(g.toByteArray))

  //u is calculated by both client and server
  def u(A: Array[Byte], B: Array[Byte]) = H(A, B)

  // g ^ x (mod N)
  def gPowXModN(x: Array[Byte]) = g modPow (BigInt(x), N)

  //Generates 32 random byte array
  def gen32RandomBytes = {
    val bytes: Array[Byte] = new Array(32)
    sr.nextBytes(bytes)
    bytes(0) = 0 //Always a positive random number
    bytes
  }
}

/**
 *  The trait that defines the calculations required on the
 *  client side- S, a, x and A
 */
trait ClientSRPParameter extends SRPParameter {
  import Util._

  //S= (B - kg^x) ^ (a + ux)   (mod N)
  def S(x: BigInt, B: BigInt, a: BigInt, u: BigInt) = {
    val bx = g.modPow(x, N)
    val btmp = ((B + N * BigInt(k)) - (BigInt(k) * bx)).mod(N)
    val Sclient = (btmp modPow (a + (u * x), N)).mod(N)
    removeBitSign(Sclient.toByteArray)
  }

  def a = gen32RandomBytes

  def x(s: Array[Byte], password: Array[Byte]) = H(s, password)

  def A(abytes: Array[Byte]) = removeBitSign(gPowXModN(abytes).toByteArray)

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
  def x(password: String) = {
    val bytes: Array[Byte] = gen32RandomBytes
    (bytes, H(bytes, password.getBytes()))
  }

  //The host stores v using:
  //v = g^x (mod N)                  (computes password verifier)
  def v(x: Array[Byte]) = gPowXModN(x).toByteArray

  def b = gen32RandomBytes

  //B = kv + g^b (mod N)
  def B(vVal: Array[Byte], bVal: Array[Byte]) =
    (BigInt(k) * BigInt(vVal) + gPowXModN(bVal)).mod(N).toByteArray

  //S = (Av^u) ^ b (mod N)
  def S(A: Array[Byte], vVal: Array[Byte], u: Array[Byte], bVal: Array[Byte]) =
    ((BigInt(vVal).modPow(BigInt(u), N)) * (BigInt(A))).mod(N).modPow(BigInt(bVal), N).mod(N).toByteArray

  //M = H(K) //Simplified, override if necessary
  def M(Kval: String) = {
    import Util._
    H(BigInt(Kval, 16).toByteArray).toHexString
  }

  //M = H(M,K) //Simplified, override if necessary
  def verifier(Kval: String, Mval: String) = {
    import Util._
    H(BigInt(Mval, 16).toByteArray, BigInt(Kval, 16).toByteArray).toHexString
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
   *  @param password The passwrod to be used to save user credentials
   *  @return Tuple3[Array[Byte],Array[Byte],Array[Byte]] s,x and v
   */
  def saveUserCredentials(userName: String, password: String) = {
    val (sVal, xVal) = x(password)
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
  def getSessionWithClientParameters(userName: String, AVal: String) = {

    val sv = findSV(userName)
    if (sv.isEmpty) {
      None
    } else {
      val (s, v) = sv.get
      val Abi = BigInt(AVal, 16)

      if (Abi == 0) throw new Exception("Invalid parameter A")

      val A = Abi.toByteArray
      val bVal = b
      val BVal = B(v, bVal)
      val uVal = u(A, BVal)
      val sessionId = S(A, v, uVal, bVal)
      Some((sessionId.toHexString, H(sessionId).toHexString, s.toHexString, BVal.toHexString))
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
