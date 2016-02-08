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
package com.actian.spark_vectorh.vector

/**
 * Error codes to return in exceptions from Vector ops
 */
object ErrorCodes {
  /** A column is non null but not loaded */
  val MissingNonNullColumn = 1
  /** Too many/few input fields for columns */
  val InvalidNumberOfInputs = 2
  /** Reference to a non-existing column */
  val NoSuchColumn = 3
  /** No columns are mapped for loading */
  val NoColumnsMapped = 4
  /** Target table does not exist */
  val NoSuchTable = 5
  /** VWLoad failed */
  val VwloadError = 6
  /** Invalid ssh credentials */
  val SshCredentialsInvalid = 7
  /** Error communicating with lead node */
  val CommunicationError = 8
  /** Address of HDFS namenode is invalid */
  val NameNodeAddressInvalid = 9
  /** Missing JDBC driver */
  val MissingDriver = 10
  /** Ran the vwinfo command and it failed */
  val VwinfoFailed = 11
  /** Error running the pre or post SQL statement(s) */
  val SqlExecutionError = 12
  /** Host for the given name does not exist */
  val NoSuchHost = 13
  /** Exception thrown by Vector during SQL execution */
  val SqlException = 14
  /** I/O error in target filesystem */
  val FileSystemError = 15
  /** Mapping from a missing input field */
  val NoSuchSourceField = 16
  /** Authentication error */
  val AuthError = 17
}
