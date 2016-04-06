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
import java.nio.ByteBuffer

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions.{ MutableRow, GenericMutableRow }

import com.actian.spark_vector.vector.ColumnMetadata
import com.actian.spark_vector.colbuffer.{ ColumnBufferBuildParams, ColumnBuffer, ReadColumnBuffer }

class RowReader(tableSchema: Seq[ColumnMetadata], vectorSize: Int, tap: DataStreamTap) extends Iterator[MutableRow] with Logging {
  import RowReader._

  /**
   * A list of read column buffers, one for each column of the unloaded table, that will be used to deserialize input `RDD` rows into the
   * buffer for the appropriate table column
   */
  private lazy val columnBufs = tableSchema.toList.map { case col =>
    logDebug(s"Trying to find a factory for column ${col.name}, type=${col.typeName}, precision=${col.precision}, scale=${col.scale}, " +
      s"nullable=${col.nullable}, vectorsize=${vectorSize}")
    ColumnBuffer.newReadBuffer(ColumnBufferBuildParams(col.name, col.typeName.toLowerCase, col.precision, col.scale, vectorSize, col.nullable))
  }

  private lazy val numColumns = tableSchema.size
  private lazy val row = new GenericMutableRow(numColumns)
  private var numTuples = 0

  private def putVectorsToColumnBufs(vector: ByteBuffer): Seq[ReadColumnBuffer[_]] = {
    if (!hasNext) throw new Exception("No more rows!")
    numTuples = vector.getInt()
    logDebug(s"Got ${numTuples} tuples to deserialize into typed column buffers.")
    columnBufs.foreach { cb =>
      cb.clear
      cb.fill(vector, numTuples) /** Consume and deserialize data from the big byte buffer */
    }
    columnBufs
  }

  private def getRow(): MutableRow = {
    var col = 0
    while (col < numColumns) {
      row(col) = if (columnBufs(col).getIsNull()) null else columnBufs(col).get()
      col += 1
    }
    row
  }

  override def hasNext(): Boolean = !tap.isEmpty || numTuples > 0

  override def next(): MutableRow = {
    var ret: MutableRow = ???
    if (numTuples == 0) putVectorsToColumnBufs(tap.read())
    numTuples -= 1
    getRow()
  }

  def close() = tap.close

  hasNext()
}

object RowReader {
  def apply(tableSchema: Seq[ColumnMetadata], vectorSize: Int, tap: DataStreamTap): RowReader = new RowReader(tableSchema, vectorSize, tap)
}
