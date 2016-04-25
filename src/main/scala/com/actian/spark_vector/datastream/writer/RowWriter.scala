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
package com.actian.spark_vector.datastream.writer

import java.sql.{ Date, Timestamp }
import java.math.BigDecimal

import org.apache.spark.Logging

import scala.reflect.{ classTag, ClassTag }

import com.actian.spark_vector.Profiling
import com.actian.spark_vector.vector.ColumnMetadata
import com.actian.spark_vector.colbuffer.{ ColumnBufferBuildParams, ColumnBuffer, WriteColumnBuffer }
import com.actian.spark_vector.datastream.{ padding, DataStreamConnectionHeader, DataStreamConnector }


/**
* Writes `RDD` rows to `ByteBuffers` and flushes them to a `Vector` through a `VectorSink`
*
* @param tableMetadataSchema schema information for the `Vector` table/relation being loaded
* @param vectorSize the max value count of the `Vector` tuple vectors
*/
class RowWriter(tableMetadataSchema: Seq[ColumnMetadata], headerInfo: DataStreamConnectionHeader, sink: DataStreamSink)
  extends Logging with Serializable with Profiling {
  import RowWriter._

  /**
   * A seq of write column buffers, one for each column of the loaded table, that will be used to serialize the
   * input `RDD` rows for the appropriate table columns
   */
  private val columnBufs = tableMetadataSchema.map { case col =>
    logDebug(s"Trying to create a write-buffer of vectorsize = ${headerInfo.vectorSize} for column = ${col.name}, type = ${col.typeName}," +
      s"precision = ${col.precision}, scale = ${col.scale}, nullable = ${col.nullable}")
    ColumnBuffer.newWriteBuffer(ColumnBufferBuildParams(col.name, col.typeName.toLowerCase, col.precision, col.scale, headerInfo.vectorSize,
      col.nullable))
  }

  /**
   * A seq of functions (one per column buffer) to write values (Any for now) in the corresponding column buffer, and performing the necessary type casts.
   *
   * @note since write column buffers are exposed only through the `WriteColumnBuffer[_]` interface and we need to cast the value(Any) to the expected type,
   * we make use of runtime reflection to determine the type of the generic parameter of the `WriteColumnBuffer[_]`
   */
  private val writeValFcns: Seq[(Any, WriteColumnBuffer[_]) => Unit] = columnBufs.map { case cb =>
    val ret: (Any, WriteColumnBuffer[_]) => Unit = cb.valueType match {
      case y if y == classTag[Byte] => writeValToColumnBuffer[Byte]
      case y if y == classTag[Short] => writeValToColumnBuffer[Short]
      case y if y == classTag[Int] => writeValToColumnBuffer[Int]
      case y if y == classTag[Long] => writeValToColumnBuffer[Long]
      case y if y == classTag[Double] => writeValToColumnBuffer[Double]
      case y if y == classTag[Float] => writeValToColumnBuffer[Float]
      case y if y == classTag[BigDecimal] => writeValToColumnBuffer[BigDecimal]
      case y if y == classTag[Boolean] => writeValToColumnBuffer[Boolean]
      case y if y == classTag[Date] => writeValToColumnBuffer[Date]
      case y if y == classTag[Timestamp] => writeValToColumnBuffer[Timestamp]
      case y if y == classTag[String] => writeValToColumnBuffer[String]
      case y => throw new Exception(s"Unexpected buffer column type '${y}'")
    }
    ret
  }

  private def writeValToColumnBuffer[T](value: Any, cb: WriteColumnBuffer[_]) = cb.asInstanceOf[WriteColumnBuffer[T]].put(value.asInstanceOf[T])

  /** Write a single `row` */
  private def writeToColumnBuffer(row: Seq[Any]): Unit = {
    var i = 0
    while (i < row.length) { // writing all columns of this row to appropiate colbufs
      writeToColumnBuffer(row(i), columnBufs(i), writeValFcns(i))
      i += 1
    }
  }

  private def writeToColumnBuffer(value: Any, cb: WriteColumnBuffer[_], writeValFcn: (Any, WriteColumnBuffer[_]) => Unit) = if (value == null) {
    cb.putNull()
  } else {
    writeValFcn(value, cb)
  }

  /**
   * After rows are buffered into column buffers, this function is called to determine the message length that will be sent through the socket. This value is equal to
   * the total amount of data buffered + a header size + some trash bytes used to properly align data types
   */
  private def bytesToBeWritten(headerSize: Int): Int = (0 until tableMetadataSchema.size).foldLeft(headerSize) { case (pos, idx) =>
    val cb = columnBufs(idx)
    pos + padding(pos, cb.alignSize) + cb.size
  }

  /**
   * Reads rows from input iterator, buffers a vector of them and then flushes all through the socket, making sure to include
   * the message length, the binary packet type `BinaryDataCode`, the number of tuples, and the actual serialized column data
   */
  def write[T <% Seq[Any]](data: Iterator[T]): Unit = {
    var i = 0
    var writtenTuples = 0
    implicit val accs = profileInit("total write", "next row", "column buffering", "writing to datastream")
    profile("total write")
    do {
      i = 0
      profile("next row")
      while (i < headerInfo.vectorSize && data.hasNext) {
        val next = data.next
        profile("column buffering")
        writeToColumnBuffer(next)
        profileEnd
        i += 1
      }
      profileEnd
      profile("writing to datastream")
      sink.write(bytesToBeWritten(DataStreamConnector.DataHeaderSize), i, columnBufs)
      writtenTuples += i
      profileEnd
    } while (i != 0)
    profileEnd
    profilePrint
  }
}

object RowWriter {
  def apply(tableSchema: Seq[ColumnMetadata], headerInfo: DataStreamConnectionHeader, sink: DataStreamSink): RowWriter = new RowWriter(tableSchema, headerInfo, sink)
}
