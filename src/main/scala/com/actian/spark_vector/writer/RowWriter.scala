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
package com.actian.spark_vector.writer

import java.sql.{ Date, Timestamp }
import java.math.BigDecimal

import org.apache.spark.Logging

import com.actian.spark_vector.vector.ColumnMetadata

import com.actian.spark_vector.colbuffer.{ColumnBufferBuildParams, ColumnBuffer}

import scala.reflect.classTag

/**
 * Writes `RDD` rows to `ByteBuffers` and flushes them to a `Vector` through a `VectorSink`
 *
 * @param tableSchema schema information for the `Vector` table/relation being loaded to
 */
class RowWriter(tableSchema: Seq[ColumnMetadata], vectorSize: Int) extends Logging {
  import RowWriter._

  /**
   * A list of column buffers, one for each column of the table inserted to that will be used to serialize input `RDD` rows into the
   * buffer for the appropriate table column
   */
  private val columnBufs = tableSchema.toList.map { case col =>
    logDebug(s"Trying to find a factory for column ${col.name}, type=${col.typeName}, precision=${col.precision}, scale=${col.scale}, " +
      s"nullable=${col.nullable}, vectorsize=${vectorSize}")
    ColumnBuffer(ColumnBufferBuildParams(col.name, col.typeName.toLowerCase, col.precision, col.scale, vectorSize, col.nullable)) match {
      case Some(cb) => cb
      case None => throw new Exception(s"Unable to find internal buffer for column ${col.name} of type ${col.typeName}")
    }
  }

  /**
   * A list of functions (one per column buffer) to write values (Any for now) into its corresponding column buffer, and performing the necessary type casts.
   *
   * @note since the column buffers are exposed only through the interface `ColumnBuf[_]`. Since we need to cast the value(Any) to the `ColumnBuf`'s expected type,
   * we make use of runtime reflection to determine the type of the generic parameter of the ColumnBuf
   */
  private val writeValFcns: Seq[(Any, ColumnBuffer[_]) => Unit] = columnBufs.map { case buf =>
    val ret: (Any, ColumnBuffer[_]) => Unit = buf.valueType match {
      case y if y == classTag[Byte] => writeValToColumnBuffer[Byte]
      case y if y == classTag[Boolean] => writeValToColumnBuffer[Boolean]
      case y if y == classTag[Short] => writeValToColumnBuffer[Short]
      case y if y == classTag[Int] => writeValToColumnBuffer[Int]
      case y if y == classTag[Long] => writeValToColumnBuffer[Long]
      case y if y == classTag[Double] => writeValToColumnBuffer[Double]
      case y if y == classTag[Float] => writeValToColumnBuffer[Float]
      case y if y == classTag[String] => writeValToColumnBuffer[String]
      case y if y == classTag[BigDecimal] => writeValToColumnBuffer[BigDecimal]
      case y if y == classTag[Date] => writeValToColumnBuffer[Date]
      case y if y == classTag[Timestamp] => writeValToColumnBuffer[Timestamp]
      case y => throw new Exception(s"Unexpected buffer column type ${y}")
    }
    ret
  }

  /** Write a single `row` */
  def write(row: Seq[Any]): Unit = (0 to row.length - 1).map {
    case idx => writeToColumnBuffer(row(idx), columnBufs(idx), writeValFcns(idx))
  }

  private def writeValToColumnBuffer[T](x: Any, columnBuf: ColumnBuffer[_]): Unit =
    columnBuf.asInstanceOf[ColumnBuffer[T]].put(x.asInstanceOf[T])

  private def writeToColumnBuffer(x: Any, columnBuf: ColumnBuffer[_], writeFcn: (Any, ColumnBuffer[_]) => Unit): Unit =
    if (x == null) {
      columnBuf.putNull()
    } else {
      writeFcn(x, columnBuf)
    }

  /**
   * After rows are buffered into column buffers, this function is called to determine the message length that will be sent through the socket. This value is equal to
   * the total amount of data buffered + a header size + some trash bytes used to properly align data types
   */
  def bytesToBeFlushed(headerSize: Int, n: Int): Int = (0 until tableSchema.size).foldLeft(headerSize) {
    case (pos, idx) =>
      val buf = columnBufs(idx)
      pos + padding(pos, buf.alignSize) + buf.size
  }

  /** Flushes buffered data to the socket through `sink` */
  def flushToSink(sink: DataStreamSink): Unit = columnBufs.foreach {
    case columnBuf => sink.write(columnBuf)
  }
}

object RowWriter {
  def apply(tableSchema: Seq[ColumnMetadata], vectorSize: Int): RowWriter = new RowWriter(tableSchema, vectorSize)

  /** Helper to determine how much padding (# of trash bytes) needs to be written to properly align a type with size `typeSize`, given that we are currently at `pos` */
  def padding(pos: Int, typeSize: Int): Int = if ((pos & (typeSize - 1)) != 0) {
    typeSize - (pos & (typeSize - 1))
  } else {
    0
  }
}
