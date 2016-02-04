package com.actian.spark_vectorh.writer

import java.sql.{ Date, Timestamp }

import org.apache.spark.Logging

import com.actian.spark_vectorh.vector.ColumnMetadata

import buffer.{ ColumnBuffer, ColumnBufferFactoriesRegistry }

/**
 * Writes `RDD` rows to `ByteBuffers` and flushes them to a `Vector(H)` through a `VectorSink`
 *
 *  @param tableSchema schema information for the `Vector(H)` table/relation being loaded to
 */
class RowWriter(tableSchema: Seq[ColumnMetadata]) extends Serializable with Logging {
  import RowWriter._

  /** Registry for finding the appropriate column buffer for each column to use for serialization purposes */
  private lazy val registry = new ColumnBufferFactoriesRegistry

  /**
   * A list of column buffers, one for each column of the table inserted to that will be used to serialize input `RDD` rows into the
   *  buffer for the appropriate table column
   */
  private lazy val columnBufs =
    tableSchema.zipWithIndex
      .map {
        case (col, idx) =>
          log.debug(s"Trying to find a factory for column ${col.name}, type=${col.typeName}, precision=${col.precision}, scale=${col.scale}," +
            s"nullable=${col.nullable} vectorsize=${DataStreamWriter.vectorSize}")
          val factory = registry.findFactoryForColumn(col.typeName, col.precision, col.scale, col.nullable)
          if (factory == null) {
            throw new Exception(s"Unable to find factory for column ${col.name} of type ${col.typeName}")
          }

          factory.createColumnBuffer(col.name, idx, col.typeName, col.precision, col.scale, col.nullable, DataStreamWriter.vectorSize)
      }

  /**
   * A list of functions (one per column buffer) to write values (Any for now) into its corresponding column buffer, and performing the necessary type casts.
   *
   *  @note since the column buffers are exposed only through the interface `ColumnBuf[_]`. Since we need to cast the value(Any) to the `ColumnBuf`'s expected type,
   *  we make use of runtime reflection to determine the type of the generic parameter of the ColumnBuf
   */
  private lazy val writeValFcns: Seq[(Any, ColumnBuffer[_]) => Unit] = columnBufs.map {
    case buf =>
      val ret: (Any, ColumnBuffer[_]) => Unit = buf.getParameterType() match {
        case y if y == classOf[java.lang.Byte] => writeValToColumnBuffer[java.lang.Byte]
        case y if y == classOf[java.lang.Boolean] => writeValToColumnBuffer[java.lang.Boolean]
        case y if y == classOf[java.lang.Short] => writeValToColumnBuffer[java.lang.Short]
        case y if y == classOf[java.lang.Integer] => writeValToColumnBuffer[java.lang.Integer]
        case y if y == classOf[java.lang.Long] => writeValToColumnBuffer[java.lang.Long]
        case y if y == classOf[java.lang.String] => writeValToColumnBuffer[java.lang.String]
        case y if y == classOf[java.lang.Number] => writeValToColumnBuffer[java.lang.Number]
        case y if y == classOf[java.sql.Date] => writeValToColumnBuffer[java.sql.Date]
        case y if y == classOf[java.lang.Double] => writeValToColumnBuffer[java.lang.Double]
        case y if y == classOf[java.lang.Float] => writeValToColumnBuffer[java.lang.Float]
        case y if y == classOf[java.sql.Timestamp] => writeValToColumnBuffer[java.sql.Timestamp]
        case y => throw new Exception(s"Unexpected buffer column type ${y.getName()}")
      }
      ret
  }

  /** Write a single `row` */
  def write(row: Seq[Any]): Unit = {
    (0 to row.length - 1).map {
      case idx =>
        writeToColumnBuffer(row(idx), columnBufs(idx), writeValFcns(idx))
    }
  }

  private def writeValToColumnBuffer[T](x: Any, columnBuf: ColumnBuffer[_]): Unit =
    columnBuf.asInstanceOf[ColumnBuffer[T]].bufferNextValue(x.asInstanceOf[T])

  private def writeToColumnBuffer(x: Any, columnBuf: ColumnBuffer[_], writeFcn: (Any, ColumnBuffer[_]) => Unit): Unit = {
    if (x == null) {
      columnBuf.bufferNextNullValue()
    } else {
      writeFcn(x, columnBuf)
    }
  }

  /**
   * After rows are buffered into column buffers, this function is called to determine the message length that will be sent through the socket. This value is equal to
   *  the total amount of data buffered + a header size + some trash bytes used to properly align data types
   */
  def bytesToBeFlushed(headerSize: Int, n: Int): Int = (0 until tableSchema.size).foldLeft(headerSize) {
    case (pos, idx) =>
      val buf = columnBufs(idx)
      pos + padding(pos + (if (tableSchema(idx).nullable) n else 0), buf.getAlignReq) + buf.getBufferSize
  }

  /** Flushes buffered data to the socket through `sink` */
  def flushToSink(sink: DataStreamSink): Unit = {
    columnBufs.foreach { case buf => buf.writeBufferedValues(sink) }
  }
}

object RowWriter {
  def apply(tableSchema: Seq[ColumnMetadata]): RowWriter = {
    new RowWriter(tableSchema)
  }

  /** Helper to determine how much padding (# of trash bytes) needs to be written to properly align a type with size `typeSize`, given that we are currently at `pos` */
  def padding(pos: Int, typeSize: Int): Int = {
    if ((pos & (typeSize - 1)) != 0) {
      typeSize - (pos & (typeSize - 1))
    } else {
      0
    }
  }
}
