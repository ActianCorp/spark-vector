package com.actian.spark_vectorh.writer

import java.io.{ ByteArrayOutputStream, DataOutputStream }
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

import scala.annotation.tailrec

import org.apache.spark.Logging

import com.actian.spark_vectorh.util.ResourceUtil.closeResourceAfterUse
import com.actian.spark_vectorh.writer.srp.VectorSRPClient

/**
 * Class containing methods to open connections to Vector(H)'s `DataStream` API
 *
 *  @param writeConf `DataStream` information containing at least the number of connections expected, the names and
 *  ports of the hosts where they are expected and authentication information
 */
class DataStreamConnector(writeConf: WriteConf) extends Logging with Serializable {
  import DataStreamConnector._

  private def openConnection(idx: Int): SocketChannel = {
    val host: VectorEndPoint = writeConf.vectorEndPoints(idx)
    log.info(s"Opening a socket to $host")
    implicit val socket = SocketChannel.open()
    socket.connect(new InetSocketAddress(host.host, host.port))
    val srpClient = new VectorSRPClient(host.username, host.password)
    srpClient.authenticate
    socket
  }

  /** Open a connection to Vector(H) and execute the code specified by `op` */
  def withConnection[T](idx: Int)(op: SocketChannel => T): T = {
    closeResourceAfterUse(openConnection(idx))(op)
  }

  def skipTableInfo(implicit socket: SocketChannel): Unit = {
    readWithByteBuffer { in => } // skip table information message. TODO(andrei): read at least the expected vectorsize
  }
}

/** This object contains methods that follow the Vector binary protocol to serialize and write data */
object DataStreamConnector extends Logging {
  // scalastyle:off magic.number
  /** Write the length `len` of a variable length message to `out` */
  @tailrec
  def writeLength(out: DataOutputStream, len: Long): Unit = {
    //log.trace("Writing length..")
    len match {
      case x if x < 255 => out.writeByte(x.toInt)
      case _ =>
        out.write(255)
        writeLength(out, len - 255)
    }
  }

  /** Write an ASCII encoded string to `out` */
  def writeString(out: DataOutputStream, s: String): Unit =
    writeByteArray(out, s.getBytes("ASCII"))

  /** Write a `ByteArray` `a` to `out` */
  def writeByteArray(out: DataOutputStream, a: Array[Byte]): Unit = {
    writeLength(out, a.length)
    out.write(a)
  }

  /**
   * Read a Vector(H) `code` from `in` and return `false` if a different
   *  code was read from the `ByteBuffer`
   */
  @tailrec
  def readCode(in: ByteBuffer, code: Array[Int]): Boolean = {
    if (in.getInt() != code(0)) {
      false
    }
    code.length == 1 || readCode(in, code.tail)
  }

  /** Read a variable lenght message's length from `in` */
  def readLength(in: ByteBuffer): Long = {
    in.get() match {
      case x if (x & 0xFF) < 255 => x & 0xFF
      case _ => 255 + readLength(in)
    }
  }

  /** Read a `ByteArray` from `in` */
  def readByteArray(in: ByteBuffer): Array[Byte] = {
    val len = readLength(in)
    //log.trace(s"Preparing to read an array of size $len")
    val ret = Array.ofDim[Byte](len.toInt)
    in.get(ret, 0, len.toInt)
    ret
  }

  /** Read an ASCII string from `in` */
  def readString(in: ByteBuffer): String = {
    new String(readByteArray(in), "ASCII")
  }

  /** Write a Vector(H) code to `out`*/
  def writeCode(out: DataOutputStream, code: Array[Int]): Unit = {
    code.foreach {
      out.writeInt(_)
    }
  }

  /** Write a `ByteBuffer` to `socket` */
  def writeByteBuffer(buffer: ByteBuffer)(implicit socket: SocketChannel): Unit = {
    buffer.flip()
    while (buffer.hasRemaining()) {
      socket.write(buffer)
    }
  }

  /** Write a `ByteBuffer` preceded by its length to `socket` */
  def writeByteBufferWithLength(buffer: ByteBuffer)(implicit socket: SocketChannel): Unit = {
    val lenByteBuffer = ByteBuffer.allocateDirect(4)
    //log.trace(s"Trying to write a byte buffer with total length of ${buffer.limit()}")
    lenByteBuffer.putInt(buffer.limit() + 4)
    writeByteBuffer(lenByteBuffer)
    buffer.position(buffer.limit())
    writeByteBuffer(buffer)
    //log.trace("Finished writing byte buffer")
  }

  /** Read a byte buffer of length `len from `socket` */
  def readByteBuffer(len: Int)(implicit socket: SocketChannel): ByteBuffer = {
    val buffer = ByteBuffer.allocate(len)
    var i = 0
    while (i < len) {
      val j = socket.read(buffer)
      if (j <= 0) throw new Exception(s"No more data to read: expecting ${len - i} bytes and read $j")
      i += j
    }
    buffer.flip()
    buffer
  }

  /** Read a byte buffer and its length integer from `socket` */
  def readByteBufferWithLength(implicit socket: SocketChannel): ByteBuffer = {
    val len = readByteBuffer(4).getInt()
    //log.trace(s"Reading a byte buffer with length: Will be reading ${len - 4} bytes")
    readByteBuffer(len - 4)
  }

  /** Write using a `ByteBuffer` to `socket`, exposing to the user a `DataOutputStream` */
  def writeWithByteBuffer(code: DataOutputStream => Unit)(implicit socket: SocketChannel): Unit = {
    val bos = new ByteArrayOutputStream()
    val out = new DataOutputStream(bos)
    code(out)
    out.flush()
    writeByteBufferWithLength(ByteBuffer.wrap(bos.toByteArray()))
    out.close()
    bos.close()
  }

  /** Read data from `socket`, store it in a `ByteBuffer` and execute the `code` that reads from it */
  def readWithByteBuffer[T](code: ByteBuffer => T)(implicit socket: SocketChannel): T = {
    val buffer = readByteBufferWithLength
    code(buffer)
  }
  // scalastyle:on magic.number
}
