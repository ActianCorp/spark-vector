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

object IntervalConversion {
  
  def convertIntervalYM(interval: String): Int = {
    var (y, m) = interval.splitAt(interval.lastIndexOf("-"))
    val years = y.toInt
    val months = m.substring(1).toInt
    if (years >= 0) (years * 12 + months)
    else (years * 12 - months)
  }
  
  def deconvertIntervalYM(source: Int): String = {
    val months = math.abs(source % 12)
    val years = source / 12
    years.toString() + "-" + months.toString()
  }
  
  def convertIntervalDS(interval: String, scale: Int): BigInteger = {
    val units = interval.split(" ")
    var totaltime = BigInteger.valueOf(units(0).toLong * SecondsInDay)
    if (scale > 0) {
      val subunits = units(1).split('.')
      val seconds: Long = subunits(0).split(":").map(_.toInt).reduceLeft((x,y) => x * 60 + y)
      val subseconds: Long = if (subunits.length > 1) subunits(1).padTo(scale, '0').toInt else  0
      totaltime = totaltime.multiply(BigInteger.valueOf(math.pow(10, scale).toLong))
      if (totaltime.compareTo(BigInteger.ZERO) < 0) 
        totaltime = totaltime.subtract(BigInteger.valueOf(seconds * math.pow(10, scale).toLong + subseconds))
      else 
        totaltime = totaltime.add(BigInteger.valueOf(seconds * math.pow(10, scale).toLong + subseconds))
    } else {
      val seconds = units(1).split(":").map(_.toInt).reduceLeft((x,y) => x * 60 + y)
      if (totaltime.compareTo(BigInteger.ZERO) < 0)
        totaltime = totaltime.subtract(BigInteger.valueOf(seconds))
      else
        totaltime = totaltime.add(BigInteger.valueOf(seconds))
    }
    totaltime
  }
  
  def deconvertIntervalDS(source: BigInteger, scale: Int): String = {
    var raw = source
    var interval = new StringBuilder()
    
    if (scale > 0) {
      val parts = raw.divideAndRemainder(BigInteger.valueOf(math.pow(10, scale).toLong))
      if (parts(1) != BigInteger.ZERO) {
        interval.append(parts(1).abs().toString().reverse.padTo(scale, '0'))
        interval.append('.')
      }
      raw = parts(0)
    }
    
    val parts = raw.divideAndRemainder(BigInteger.valueOf(SecondsInDay))
    var seconds = math.abs(parts(1).longValue())
    val multiples = Seq(SecondsInMinute, MinutesInHour, HoursInDay)
    val time = for (m <- multiples) yield {
      val r = seconds % m
      seconds /= m
      r.toString().reverse.padTo(2, '0')
    }
    interval.append(time.mkString(":")).append(" ").append(parts(0).toString().reverse)
    interval.reverse.toString()
  }
}