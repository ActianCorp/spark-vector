package com.actian.spark_vectorh.writer

import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

import buffer.VectorSink

case class DataStreamSink(implicit socket: SocketChannel) extends VectorSink {
  import DataStreamWriter._
  var pos: Int = 0

  def writeColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer, align_size: Int): Unit = {
    if (markers != null) {
      writeByteBuffer(markers)
      pos = pos + markers.limit()
    }
    align(align_size)
    writeByteBuffer(values)
    pos = pos + values.limit()
  }

  private def align(typeSize: Int): Unit = {
    RowWriter.padding(pos, typeSize) match {
      case x if x > 0 =>
        writeByteBuffer(ByteBuffer.allocateDirect(x))
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
