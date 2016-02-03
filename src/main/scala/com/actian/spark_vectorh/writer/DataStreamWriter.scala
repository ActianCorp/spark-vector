package com.actian.spark_vectorh.writer

import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

import scala.concurrent.Future

import org.apache.spark.{ Logging, TaskContext }

import com.actian.spark_vectorh.vector.VectorConnectionProperties

class DataStreamWriter[T <% Seq[Any]](
    vectorProps: VectorConnectionProperties,
    table: String,
    rowWriter: RowWriter) extends Logging with Serializable {
  import DataStreamConnector._
  import DataStreamWriter._

  @transient
  val client = DataStreamClient(vectorProps, table)
  lazy val writeConf = {
    client.prepareDataStreams
    client.getWriteConf
  }
  private lazy val connector = new DataStreamConnector(writeConf)
  private val binaryDataCode = 5 /* X100CPT_BINARY_DATA_V2 */

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
    profilePrint(log)
  }

  def write(taskContext: TaskContext, data: Iterator[T]): Unit = {
    connector.withConnection(taskContext.partitionId)(implicit channel => {
      implicit val sink = DataStreamSink()
      connector.skipTableInfo
      writeSplittingInVectors(data)
    })
  }

  def initiateLoad: Future[Int] = client.startLoad

  def commit: Unit = client.commit
}

object DataStreamWriter {
  val vectorSize = 1024

  def writeByteBuffer(buffer: ByteBuffer)(implicit socket: SocketChannel): Unit = {
    while (buffer.hasRemaining())
      socket.write(buffer)
  }

  def writeInt(x: Int)(implicit socket: SocketChannel): Unit = {
    val buffer = ByteBuffer.allocateDirect(intSize)
    buffer.putInt(x)
    buffer.flip()
    writeByteBuffer(buffer)
  }
}
