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
package com.actian.spark_vector.provider

import scala.util.Random
import com.actian.spark_vector.srp.VectorSRPServer

case class ProviderAuth(username: String, password: String) {
  val srpServer = {
    val ret = new VectorSRPServer
    ret.saveUserCredentials(username, password)
    ret
  }

  def doAuthentication: Boolean = !username.isEmpty && !password.isEmpty
}

object ProviderAuth {
  /**
   * Generate a random Ascii string containing alpha numeric characters + some non alpha numeric characters as specified by `nonAlphaNumericChars`
   *
   * @param nonAlphaNumericCharPercentage percentage of non alpha numeric characters in the generated string
   */
  def randomAsciiString(n: Int, nonAlphaNumericChars: String = "", nonAlphaNumericCharPercentage: Double = .1): String = if (nonAlphaNumericChars.length == 0) {
    Random.alphanumeric.take(n).mkString
  } else {
    val alphaPercentage = 1.0 - nonAlphaNumericCharPercentage
    val alphaNumericN = (alphaPercentage * n).toInt
    Random.shuffle(Random.alphanumeric.take(alphaNumericN).toSeq ++
      Stream.continually(Random.nextInt(nonAlphaNumericChars.length)).map(nonAlphaNumericChars).take(n - alphaNumericN).toSeq).mkString
  }

  /**
   * Generate a username to be used when authenticating with the provider.
   *
   * @note the generated username will always start with a prefix 'prov_'
   */
  def generateUsername: String = {
    val Prefix = "prov_"
    val UsernameMaxLen = 63
    val size = UsernameMaxLen - Prefix.length
    s"${Prefix}${randomAsciiString(size)}"
  }

  /**
   * Generates a password to be used when authenticated with the provider.
   *
   * @note the generated password will be composed of alpha numeric characters + '!@#$%^&'
   */
  def generatePassword: String = {
    val PasswordMaxLen = 255
    randomAsciiString(PasswordMaxLen, "!@#$%^&")
  }
}
