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
package com.actian.spark_vector.datastream

import com.actian.spark_vector.vector.VectorJDBC

/** Configuration for read/write end points - one entry for each Vector end point expecting data */
case class VectorEndpointConf(vectorEndpoints: IndexedSeq[VectorEndpoint]) extends Serializable

object VectorEndpointConf {
  def apply(jdbc: VectorJDBC): VectorEndpointConf = VectorEndpointConf(VectorEndpoint.fromDataStreamsTable(jdbc))
}
