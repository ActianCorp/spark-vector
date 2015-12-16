package com.actian.spark_vectorh.writer

import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

import buffer.VectorSink

case class DataStreamSink(implicit socket: SocketChannel) extends VectorSink {
  import DataStreamWriter._
  var pos: Int = 0

  def writeColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer): Unit = {
    writeByteBuffer(values)
    pos = pos + values.limit()
    if (markers != null) {
      writeByteBuffer(markers)
      pos = pos + markers.limit()
    }
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
    writeColumn(columnIndex, values, markers)

  def writeShortColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer): Unit = {
    align(2)
    writeColumn(columnIndex, values, markers)
  }

  def writeIntColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer): Unit = {
    align(4)
    writeColumn(columnIndex, values, markers)
  }

  def writeLongColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer): Unit = {
    align(8)
    writeColumn(columnIndex, values, markers)
  }

  def write128BitColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer): Unit = {
    align(16)
    writeColumn(columnIndex, values, markers)
  }

  def writeFloatColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer): Unit = {
    align(4)
    writeColumn(columnIndex, values, markers)
  }

  def writeDoubleColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer): Unit = {
    align(8)
    writeColumn(columnIndex, values, markers)
  }

  def writeStringColumn(columnIndex: Int, values: ByteBuffer, markers: ByteBuffer): Unit = {
    writeColumn(columnIndex, values, markers)
  }
  // scalastyle:on magic.number
}
