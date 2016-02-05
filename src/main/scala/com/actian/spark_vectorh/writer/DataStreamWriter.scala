package com.actian.spark_vectorh.writer

import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import scala.concurrent.Future
import org.apache.spark.{ Logging, TaskContext }
import com.actian.spark_vectorh.vector.VectorConnectionProperties
import java.io.DataOutputStream
import java.io.ByteArrayOutputStream
import scala.annotation.tailrec
import com.actian.spark_vectorh.Profiling

/**
 * Entry point for loading with spark-vectorh connector.
 *
 * @param vectorProps connection information to the leader node's SQL interface
 * @param table The table loaded to
 * @param rowWriter used to write rows consumed from input `RDD` to `ByteBuffer`s and then flushed through the socket to `Vector(H)`
 */
class DataStreamWriter[T <% Seq[Any]](
    vectorProps: VectorConnectionProperties,
    table: String,
    rowWriter: RowWriter) extends Logging with Serializable with Profiling {

  import DataStreamWriter._

  /**
   * A client to connect to the `Vector(H)`'s SQL interface through JDBC. Currently used to
   * obtain `DataStream` connection information (# of streams, hosts, roles, etc.) and to submit the load query.
   *
   * @note Available only on the driver.
   */
  @transient
  val client = DataStreamClient(vectorProps, table)
  /** Write configuration to be used when connecting to the `DataStream` API */
  lazy val writeConf = {
    client.prepareDataStreams
    client.getWriteConf
  }
  private lazy val connector = new DataStreamConnector(writeConf)
  private val binaryDataCode = 5 /* X100CPT_BINARY_DATA_V2 */

  /**
   * Read rows from input iterator, buffer a vector of them and then flush them through the socket, making sure to include
   * the message length, the binary packet type `binaryDataCode`, the number of tuples, and the actual serialized data
   */
  private def writeSplittingInVectors(data: Iterator[T])(implicit sink: DataStreamSink) = {
    implicit val socket = sink.socket
    var i = 0
    var written = 0
    val headerSize = 4 /* code */ + 4 /* number of tuples */ + 4 /* messageLength */
    implicit val accs = profileInit("total", "child", "buffering", "flushing")
    profile("total")
    do {
      i = 0
      profile("child")
      while (i < vectorSize && data.hasNext) {
        val next = data.next
        profile("buffering")
        rowWriter.write(next)
        profileEnd
        i = i + 1
      }
      profileEnd
      profile("flushing")
      writeInt(rowWriter.bytesToBeFlushed(headerSize, i))
      writeInt(binaryDataCode)
      writeInt(i) // write actual number of tuples
      sink.pos = headerSize
      rowWriter.flushToSink(sink)
      written = written + i
      profileEnd
    } while (i != 0)
    profileEnd
    profilePrint
  }

  /**
   * This function is executed once for each partition of `DataStreamRDD` and it will open a socket connection, process all data
   * assigned to its corresponding partition (`taskContext.partitionId`) and then close the connection.
   */
  def write(taskContext: TaskContext, data: Iterator[T]): Unit = {
    connector.withConnection(taskContext.partitionId)(implicit channel => {
      implicit val sink = DataStreamSink()
      connector.skipTableInfo
      writeSplittingInVectors(data)
    })
  }

  /**
   * Initiate the load, i.e. submit the SQL query to start loading from an external source.
   *
   * @note should only be called on the driver.
   */
  def initiateLoad: Future[Int] = client.startLoad

  /**
   * Commit the last transaction started by this `DataStreamWriter`'s client.
   *
   * @note should only be called on the driver.
   */
  def commit: Unit = client.commit
}

/** Contains helpers to write binary data, conforming to `Vector(H)`'s binary protocol */
object DataStreamWriter {
  /** Default vector size to use while loading. i.e. the number of rows that will be transmitted with each message sent to `Vector(H)` */
  val vectorSize = 1024

  // scalastyle:off magic.number
  /** Write the length `len` of a variable length message to `out` */
  @tailrec
  def writeLength(out: DataOutputStream, len: Long): Unit = {
    //logTrace("Writing length..")
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
   * Writes `buffer` to `socket`. Note this method assumes that the buffer is in read mode, i.e.
   * the position is at 0. To flip a `ByteBuffer` before writing, use `writeByteBuffer`
   */
  def writeByteBufferNoFlip(buffer: ByteBuffer)(implicit socket: SocketChannel): Unit = {
    while (buffer.hasRemaining())
      socket.write(buffer)
  }

  /** Writes an integer to the socket */
  def writeInt(x: Int)(implicit socket: SocketChannel): Unit = {
    val buffer = ByteBuffer.allocateDirect(intSize)
    buffer.putInt(x)
    writeByteBuffer(buffer)
  }

  /** Write a Vector(H) code to `out`*/
  def writeCode(out: DataOutputStream, code: Array[Int]): Unit = {
    code.foreach {
      out.writeInt(_)
    }
  }

  /**
   * Write a `ByteBuffer` to `socket`. Note this method flips the byteBuffer before writing. For writing
   * a `ByteBuffer` without flipping, use `writeByteBufferNoFlip`
   */
  def writeByteBuffer(buffer: ByteBuffer)(implicit socket: SocketChannel): Unit = {
    buffer.flip()
    writeByteBufferNoFlip(buffer)
  }

  /** Write a `ByteBuffer` preceded by its length to `socket` */
  def writeByteBufferWithLength(buffer: ByteBuffer)(implicit socket: SocketChannel): Unit = {
    val lenByteBuffer = ByteBuffer.allocateDirect(4)
    //logTrace(s"Trying to write a byte buffer with total length of ${buffer.limit()}")
    lenByteBuffer.putInt(buffer.limit() + 4)
    writeByteBuffer(lenByteBuffer)
    buffer.position(buffer.limit())
    writeByteBuffer(buffer)
    //logTrace("Finished writing byte buffer")
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
  // scalastyle:on magic.number
}
