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

import org.apache.spark.Logging

import com.actian.spark_vector.util.ResourceUtil.closeResourceOnFailure
import com.actian.spark_vector.colbuffer.ColumnBuffer

import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

/** The `VectorTap` that reads a `Vector DataStream` from a socket as a `ByteBuffer` */
private[reader] case class DataStreamTap(implicit val socket: SocketChannel) extends Logging with Serializable {
  import DataStreamReader._

  private val BinaryDataCode = 5 /* X100CPT_BINARY_DATA_V2 */
  private val NumTuplesIndex = 4

  private var vectors: ByteBuffer = null
  private var tapOpened: Boolean = true
  private var remaining = true

  private def readVectors(reuseByteBuffer: ByteBuffer): ByteBuffer = readWithByteBuffer(Option(reuseByteBuffer)) { vectors =>
    val dataCode = vectors.getInt()
    if (dataCode != BinaryDataCode) throw new Exception(s"Invalid binary data code = ${dataCode}!")
    if (vectors.getInt(NumTuplesIndex) == 0) {
      logDebug(s"Empty data stream.")
      remaining = false
      null
    } else {
      vectors
    }
  }

  def read()(implicit reuseByteBuffer: ByteBuffer): ByteBuffer = {
    if (!remaining) throw new NoSuchElementException("Empty data stream.")
    if (!tapOpened) {
      tapOpened = true
    } else {
      vectors = readVectors(reuseByteBuffer)
    }
    vectors
  }

  def isEmpty()(implicit reuseByteBuffer: ByteBuffer): Boolean = {
    if (remaining) read()
    tapOpened = false
    !remaining
  }

  def close() = socket.close
}
