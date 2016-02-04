package com.actian.spark_vectorh.writer

import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

import buffer.VectorSink

/** The `VectorSink` that flushes `ByteBuffers` through the `SocketChannel` `socket` to a `Vector(H) DataStream` */ 
case class DataStreamSink(implicit socket: SocketChannel) extends VectorSink {
  import DataStreamWriter._
  /** The write position (how many bytes have already been written to `socket`). Used to
    *  calculate data type alignments */
  var pos: Int = 0

  private def writeColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer, align_size: Int): Unit = {
    if (markers != null) {
      writeByteBufferNoFlip(markers)
      pos = pos + markers.limit()
    }
    align(align_size)
    writeByteBufferNoFlip(values)
    pos = pos + values.limit()
  }

  private def align(typeSize: Int): Unit = {
    RowWriter.padding(pos, typeSize) match {
      case x if x > 0 =>
        writeByteBufferNoFlip(ByteBuffer.allocateDirect(x))
        pos = pos + x
      case _ =>
    }
  }
  
  // scalastyle:off magic.number
  def writeByteColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer): Unit =
    writeColumn(columnIndex, values, markers, 1)

  def writeShortColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer): Unit =
    writeColumn(columnIndex, values, markers, 2)

  def writeIntColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer): Unit =
    writeColumn(columnIndex, values, markers, 4)

  def writeLongColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer): Unit =
    writeColumn(columnIndex, values, markers, 8)

  def write128BitColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer): Unit =
    writeColumn(columnIndex, values, markers, 16)

  def writeFloatColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer): Unit =
    writeColumn(columnIndex, values, markers, 4)

  def writeDoubleColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer): Unit =
    writeColumn(columnIndex, values, markers, 8)

  def writeStringColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer): Unit =
    writeColumn(columnIndex, values, markers, 1)
  // scalastyle:on magic.number
}
