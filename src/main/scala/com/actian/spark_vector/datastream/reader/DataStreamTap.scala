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

import com.actian.spark_vector.util.ResourceUtil.closeResourceOnFailure
import com.actian.spark_vector.colbuffer.ColumnBuffer
import com.actian.spark_vector.util.Logging

import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

/** The `VectorTap` that reads `Vector` vectors from a `Vector DataStream`'s socket as `ByteBuffer`s */
private[reader] case class DataStreamTap(implicit val socket: SocketChannel) extends Logging with Serializable {
  import DataStreamReader._

  private final val SuccessCode = 0 // X100CPT_SUCCESS
  private final val BinaryDataCode = 5 // X100CPT_BINARY_DATA_V2
  private final val NumTuplesIndex = 4

  private var vector: ByteBuffer = _
  private var isNextVectorBuffered = false
  private var remaining = true

  private def readVector(reuseBuffer: ByteBuffer): ByteBuffer = readWithByteBuffer(Option(reuseBuffer)) { vectors =>
    val packetType = vectors.getInt()
    if (packetType != BinaryDataCode && packetType != SuccessCode) throw new Exception(s"Invalid packet type code = ${packetType}.")
    if (vectors.getInt(NumTuplesIndex) == 0) {
      logDebug(s"Empty data stream.")
      remaining = false
    }
    vectors
  }

  def read()(implicit reuseBuffer: ByteBuffer): ByteBuffer = {
    if (!remaining) throw new NoSuchElementException("Empty data stream.")
    if (isNextVectorBuffered) {
      isNextVectorBuffered = false
    } else {
      vector = readVector(reuseBuffer)
    }
    vector
  }

  def isEmpty()(implicit reuseBuffer: ByteBuffer): Boolean = {
    if (remaining) read()
    isNextVectorBuffered = true
    !remaining
  }

  def close(): Unit = socket.close
}
