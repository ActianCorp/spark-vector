package com.actian.spark_vectorh.writer

import java.io.{ ByteArrayOutputStream, DataOutputStream }
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

import scala.annotation.tailrec

import org.apache.spark.Logging

import com.actian.spark_vectorh.util.ResourceUtil.closeResourceAfterUse
import com.actian.spark_vectorh.writer.srp.VectorSRPClient

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

  def withConnection[T](idx: Int)(cxn: SocketChannel => T): T = {
    closeResourceAfterUse(openConnection(idx))(cxn)
  }

  def skipTableInfo(implicit socket: SocketChannel): Unit = {
    readWithByteBuffer { in => } // column definition header
    readWithByteBuffer { in => } // actual data
    readWithByteBuffer { in => } // number of tuples
    readWithByteBuffer { in => } // ready for data message
  }
}

object DataStreamConnector extends Logging {
  // scalastyle:off magic.number
  def writeLength(out: DataOutputStream, len: Long): Unit = {
    //log.trace("Writing length..")
    len match {
      case x if x < 255 => out.writeByte(x.toInt)
      //log.trace(s"Wrote $x")
      case _ =>
        out.write(255)
        //log.trace(s"Wrote 255")
        writeLength(out, len - 255)
    }
  }

  def writeString(out: DataOutputStream, s: String): Unit =
    writeByteArray(out, s.getBytes("ASCII"))

  def writeByteArray(out: DataOutputStream, a: Array[Byte]): Unit = {
    writeLength(out, a.length)
    out.write(a)
  }

  @tailrec
  def readCode(in: ByteBuffer, code: Array[Int]): Boolean = {
    if (in.getInt() != code(0)) {
      false
    }
    code.length == 1 || readCode(in, code.tail)
  }

  def readLength(in: ByteBuffer): Long = {
    in.get() match {
      case x if (x & 0xFF) < 255 => x & 0xFF
      case _ => 255 + readLength(in)
    }
  }

  def readByteArray(in: ByteBuffer): Array[Byte] = {
    val len = readLength(in)
    //log.trace(s"Preparing to read an array of size $len")
    val ret = Array.ofDim[Byte](len.toInt)
    in.get(ret, 0, len.toInt)
    ret
  }

  def readString(in: ByteBuffer): String = {
    new String(readByteArray(in), "ASCII")
  }

  def writeCode(out: DataOutputStream, code: Array[Int]): Unit = {
    code.foreach {
      out.writeInt(_)
    }
  }

  def writeByteBuffer(buffer: ByteBuffer)(implicit socket: SocketChannel): Unit = {
    //log.trace("Writing byte buffer.... Wait on it")
    buffer.flip()
    while (buffer.hasRemaining()) {
      socket.write(buffer)
    }
  }

  def writeByteBufferWithLength(buffer: ByteBuffer)(implicit socket: SocketChannel): Unit = {
    val lenByteBuffer = ByteBuffer.allocateDirect(4)
    //log.trace(s"Trying to write a byte buffer with total length of ${buffer.limit()}")
    lenByteBuffer.putInt(buffer.limit() + 4)
    writeByteBuffer(lenByteBuffer)
    buffer.position(buffer.limit())
    writeByteBuffer(buffer)
    //log.trace("Finished writing byte buffer")
  }

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

  def readByteBufferWithLength(implicit socket: SocketChannel): ByteBuffer = {
    val len = readByteBuffer(4).getInt()
    //log.trace(s"Reading a byte buffer with length: Will be reading ${len - 4} bytes")
    readByteBuffer(len - 4)
  }

  def writeWithByteBuffer(code: DataOutputStream => Unit)(implicit socket: SocketChannel): Unit = {
    val bos = new ByteArrayOutputStream()
    val out = new DataOutputStream(bos)
    code(out)
    out.flush()
    writeByteBufferWithLength(ByteBuffer.wrap(bos.toByteArray()))
    out.close()
    bos.close()
  }

  def readWithByteBuffer[T](code: ByteBuffer => T)(implicit socket: SocketChannel): T = {
    val buffer = readByteBufferWithLength
    code(buffer)
  }
  // scalastyle:on magic.number
}
