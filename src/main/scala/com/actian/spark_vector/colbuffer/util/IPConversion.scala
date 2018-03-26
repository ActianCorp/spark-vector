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
package com.actian.spark_vector.colbuffer.util

import java.math.BigInteger
import java.net.InetAddress

object IPConversion {
  
  def ipv4IntegerToString(i: Int): String = {
    val p = i ^ (1 << 31)
    Array(((p >> 24) & 0xFF), ((p >> 16) & 0xFF), ((p >> 8) & 0xFF), ( p & 0xFF)).mkString(".")
  }
  
  def ipv4StringToInteger(ipstr: String): Int = {
    (ipstr.split("\\.").reverse.zipWithIndex.map(p => p._1.toInt * math.pow(256 , p._2).toLong).sum ^ (1 << 31)).toInt
  }
  
  def ipv6LongsToString(lower: Long, upper: Long): String = {
    var l1 = lower
    var l2 = upper ^ (1l << 63)
    val iparr = for (i <- 0 until 8) yield {
      if (i < 4) {
        val s = (l1 & 0xFFFF).toHexString
        l1 = l1 >> 16
        s
      } else {
        val s = (l2 & 0xFFFF).toHexString
        l2 = l2 >> 16
        s
      }
    }
    var ip = iparr.reverse.mkString(":")
    
    if (ip.contains("0:0")) {
      ip = ip.replaceAll("0(:0)+", ":")
      if (ip.length == 1)
        ip += ":"
    }
    ip
  }
  
  def ipv6StringToLongs(ipstr: String): (Long, Long) = {
    val addr = InetAddress.getByName(ipstr)
    val value = new BigInteger(1, addr.getAddress)
    val lower = value.longValue()
    val upper = (value.shiftRight(64)).longValue() ^ (1l << 63)
    (lower, upper)
  }
}