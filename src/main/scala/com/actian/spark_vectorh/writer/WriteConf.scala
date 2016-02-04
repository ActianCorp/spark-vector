package com.actian.spark_vectorh.writer

import com.actian.spark_vectorh.vector.VectorJDBC

/** Configuration for writing, one entry for each Vector end point expecting data */
case class WriteConf(vectorEndPoints: IndexedSeq[VectorEndPoint]) extends Serializable

object WriteConf {
  def apply(jdbc: VectorJDBC): WriteConf = {
    WriteConf(VectorEndPoint.fromDataStreamsTable(jdbc))
  }
}
