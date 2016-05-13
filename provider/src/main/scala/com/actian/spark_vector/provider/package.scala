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
package com.actian.spark_vector

import scala.util.Random

package object provider {
  def randomAsciiString(n: Int, nonAlphaNumericChars: String = "", nonAlphaNumericCharPercentage: Double = .1): String = if (nonAlphaNumericChars.length == 0) {
    Random.alphanumeric.take(n).mkString
  } else {
    val alphaPercentage = 1.0 - nonAlphaNumericCharPercentage
    val alphaNumericN = (alphaPercentage * n).toInt
    Random.shuffle(Random.alphanumeric.take(alphaNumericN).toSeq ++
      Stream.continually(Random.nextInt(nonAlphaNumericChars.length)).map(nonAlphaNumericChars).take(n - alphaNumericN).toSeq).mkString
  }

  def generateUsername: String = {
    val prefix = "prov_"
    val UsernameLen = 63
    val size = UsernameLen - prefix.length
    "prov_" + randomAsciiString(size)
  }

  def generatePassword: String = {
    val PasswordLen = 255
    randomAsciiString(PasswordLen, "!@#$%^&")
  }
}
