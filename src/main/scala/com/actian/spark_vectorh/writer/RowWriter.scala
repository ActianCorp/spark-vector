package com.actian.spark_vectorh.writer

import java.sql.{ Date, Timestamp }

import org.apache.spark.Logging

import com.actian.spark_vectorh.vector.ColumnMetadata

import buffer.{ ColumnBuffer, ColumnBufferFactoriesRegistry }

class RowWriter(tableSchema: Seq[ColumnMetadata]) extends Serializable with Logging {
  import RowWriter._

  private lazy val registry = new ColumnBufferFactoriesRegistry

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

  def bytesToBeFlushed(headerSize: Int, n: Int): Int = (0 until tableSchema.size).foldLeft(headerSize) {
    case (pos, idx) =>
      val buf = columnBufs(idx)
      log.info(s"Aligning position $pos to ${padding(pos, buf.getAlignReq)} and adding buffer size ${buf.getBufferSize}")
      pos + padding(pos + (if (tableSchema(idx).nullable) n else 0), buf.getAlignReq) + buf.getBufferSize
  }

  def flushToSink(sink: DataStreamSink): Unit = {
    columnBufs.foreach { case buf => buf.writeBufferedValues(sink) }
  }
}

object RowWriter {
  def apply(tableSchema: Seq[ColumnMetadata]): RowWriter = {
    new RowWriter(tableSchema)
  }
  def padding(pos: Int, typeSize: Int): Int = {
    if ((pos & (typeSize - 1)) != 0) {
      typeSize - (pos & (typeSize - 1))
    } else {
      0
    }
  }
}
