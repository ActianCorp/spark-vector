package com.actian.spark_vectorh.writer

import com.actian.spark_vectorh.vector.VectorJDBC

case class WriteConf(vectorEndPoints: IndexedSeq[VectorEndPoint],
  parallelismLevel: Int = WriteConf.DefaultParallelismLevel) extends Serializable

object WriteConf {

  val DefaultParallelismLevel = 2

  def apply(jdbc: VectorJDBC): WriteConf = {
    WriteConf(VectorEndPoint.fromDataStreamsTable(jdbc))
  }
}
