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
package com.actian.spark_vector.datastream.reader

import java.sql.{ Date, Timestamp }
import java.math.BigDecimal
import java.nio.{ ByteOrder, ByteBuffer }

import scala.reflect.{ classTag, ClassTag }

import org.apache.spark.Logging
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow
import org.apache.spark.sql.catalyst.InternalRow

import com.actian.spark_vector.Profiling
import com.actian.spark_vector.vector.ColumnMetadata
import com.actian.spark_vector.colbuffer.{ ByteSize, ColumnBufferBuildParams, ColumnBuffer, ReadColumnBuffer }
import com.actian.spark_vector.datastream.{ padding, DataStreamConnectionHeader, DataStreamConnector }

class RowReader(tableMetadataSchema: Seq[ColumnMetadata], headerInfo: DataStreamConnectionHeader, tap: DataStreamTap)
  extends Iterator[InternalRow] with Logging with Serializable with Profiling {
  import RowReader._

  implicit val accs = profileInit("reading from datastream", "columns buffering")

  private val row = new SpecificMutableRow(tableMetadataSchema.map(_.dataType))
  private val numColumns = tableMetadataSchema.size
  private var numTuples = 0

  /**
   * A seq of read column buffers, one for each column of the unloaded table, that will be used to deserialize the data
   * streams for the appropriate table columns
   */
  private val columnBufs = tableMetadataSchema.zipWithIndex.map { case (col, i) =>
    logDebug(s"Trying to create a read-buffer of vectorsize = ${headerInfo.vectorSize} for column = ${col.name}, type = ${col.typeName}, " +
      s"precision = ${col.precision}, scale = ${col.scale}, nullable = ${headerInfo.isNullableCol(i)}, constant = ${headerInfo.isConstCol(i)}")
    ColumnBuffer.newReadBuffer(ColumnBufferBuildParams(col.name, col.typeName.toLowerCase, col.precision, col.scale,
      (if (headerInfo.isConstCol(i)) 1 else headerInfo.vectorSize), headerInfo.isNullableCol(i)))
  }

  private val reuseBufferSize = bytesToBeRead(DataStreamConnector.DataHeaderSize)

  private implicit val reuseBuffer: ByteBuffer = ByteBuffer.allocateDirect(reuseBufferSize)

  private def bytesToBeRead(headerSize: Int): Int = (0 until tableMetadataSchema.size).foldLeft(headerSize) { case (pos, idx) =>
    val cb = columnBufs(idx)
    pos + padding(pos, cb.alignSize) + cb.maxValueCount * ((if (cb.nullable) ByteSize else 0) + tableMetadataSchema(idx).maxDataSize)
  }

  private def fillColumnBuffers(vector: ByteBuffer) = {
    numTuples = vector.getInt()
    vector.order(ByteOrder.LITTLE_ENDIAN) /** The data from Vector comes in LITTLE_ENDIAN */
    columnBufs.foreach { cb =>
      cb.clear
      cb.fill(vector, numTuples) /** Consume and deserialize tuples from the vector byte buffer */
    }
    vector.order(ByteOrder.BIG_ENDIAN) /** Go back to BIG_ENDIAN because this buffer is reused by DataStreamTap */
    columnBufs
  }

  /**
   * A list of calls (one per column buffer) to set typed values in the corresponding row's column position, and performing the necessary type casts
   * while reading the needed value from its `ReadColumnBuffer[_]`.
   *
   * @note since read column buffers are exposed only through the `ReadColumnBuffer[_]` interface and we need to set the `SpecificMutableRow` with appropriate
   * typed values, we make use of runtime reflection to determine the type of the generic parameter of the `ReadColumnBuffer[_]`
   */
  private def setValFromColumnBuffer(col: Int) = columnBufs(col).valueType match {
    case y if y == classTag[Byte] => row.setByte(col, readValFromColumnBuffer[Byte](col))
    case y if y == classTag[Short] => row.setShort(col, readValFromColumnBuffer[Short](col))
    case y if y == classTag[Int] => row.setInt(col, readValFromColumnBuffer[Int](col))
    case y if y == classTag[Long] => row.setLong(col, readValFromColumnBuffer[Long](col))
    case y if y == classTag[Float] => row.setFloat(col, readValFromColumnBuffer[Float](col))
    case y if y == classTag[Double] => row.setDouble(col, readValFromColumnBuffer[Double](col))
    case y if y == classTag[BigDecimal] => row.update(col, Decimal(readValFromColumnBuffer[BigDecimal](col)))
    case y if y == classTag[Boolean] => row.setBoolean(col, readValFromColumnBuffer[Boolean](col))
    case y if y == classTag[UTF8String] => row.update(col, readValFromColumnBuffer[UTF8String](col))
    case y => throw new Exception(s"Unexpected buffer column type '${y}'")
  }

  private def readValFromColumnBuffer[T: ClassTag](col: Int) = columnBufs(col).asInstanceOf[ReadColumnBuffer[T]].get()

  private def read(): InternalRow = {
    var col = 0
    while (col < numColumns) {
      setValAt(col)
      col += 1
    }
    row
  }

  private def setValAt(col: Int) = if (columnBufs(col).isNextNull) {
    row.setNullAt(col)
  } else {
    setValFromColumnBuffer(col)
  }

  override def hasNext(): Boolean = numTuples > 0 || {
    profile("reading from datastream")
    val ret = !tap.isEmpty
    profileEnd
    ret
  }

  override def next(): InternalRow = {
    if (!hasNext) throw new NoSuchElementException("Empty row reader.")
    if (numTuples == 0) {
      val vector = tap.read()
      profile("columns buffering")
      fillColumnBuffers(vector)
      profileEnd
    }
    numTuples -= 1
    read()
  }

  def close(): Unit = {
    profilePrint
    tap.close
  }
}

object RowReader {
  def apply(tableSchema: Seq[ColumnMetadata], headerInfo: DataStreamConnectionHeader, tap: DataStreamTap): RowReader = new RowReader(tableSchema, headerInfo, tap)
}
