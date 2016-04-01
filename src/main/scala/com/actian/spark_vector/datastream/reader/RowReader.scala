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
import org.apache.spark.sql.Row
import com.actian.spark_vector.vector.ColumnMetadata
import com.actian.spark_vector.colbuffer.{ ColumnBufferBuildParams, ColumnBuffer }
import org.apache.spark.sql.RowFactory

class RowReader(tableSchema: Seq[ColumnMetadata], vectorSize: Int, tap: DataStreamTap) extends Iterator[Row] with Logging {
  import RowReader._

  /**
   * TODO: duplicate - RowWriter
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
  private val numColumns = tableSchema.size
  private val row = collection.mutable.IndexedSeq.fill[Any](numColumns)(null)
  private var numTuples = 0

  private def putVectorsToColumnBufs(vector: ByteBuffer): Seq[ColumnBuffer[_]] = {
    if (!hasNext) throw new Exception("No more rows!")
    numTuples = vector.getInt()
    columnBufs.foreach { cb =>
      cb.clear
      cb.put(vector, numTuples) /** Consume and deserialize data from the big byte buffer */
    }
    columnBufs
  }

  private def getRow(): Row = {
    var col = 0
    while (col < numColumns) {
      row(col) = columnBufs(col).get()
      col += 1
    }
    Row.fromSeq(row) /* refolosit */
  }

  override def hasNext(): Boolean = !tap.isEmpty || numTuples > 0

  override def next(): Row = {
    var ret: Row = ???
    if (numTuples > 0) {
      ret = getRow()
    } else {
      putVectorsToColumnBufs(tap.read())
      ret = getRow()
    }
    numTuples -= 1
    ret
  }

  def close() = tap.close

  hasNext()
}

object RowReader {
  def apply(tableSchema: Seq[ColumnMetadata], vectorSize: Int, tap: DataStreamTap): RowReader = new RowReader(tableSchema, vectorSize, tap)
}
