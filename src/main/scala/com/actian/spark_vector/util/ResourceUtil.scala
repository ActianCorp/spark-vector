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
package com.actian.spark_vector.util

import scala.language.reflectiveCalls

import resource.ExtractableManagedResource

object ResourceUtil {
  import resource._

  /**
   * Wrap an `ExtractableManagedResource[T]` into a new class providing the capability to map errors using an
   * function specified by `excMapper`
   */
  implicit class RichExtractableManagedResource[T](val res: ExtractableManagedResource[T]) extends AnyVal {
    def resolve(excMapper: Throwable => Throwable = identity): T = res.either.fold(e => throw excMapper(e.head), identity)
  }

  /** Close a resource after using it, regardless of whether an exception was thrown or not */
  def closeResourceAfterUse[T, C <: { def close() }](closeable: C*)(code: => T): T = try code finally {
    closeable.foreach { _.close }
  }

  /** Close a resource if an exception was thrown */
  def closeResourceOnFailure[T, C <: { def close() }](closeable: C*)(code: => T): T = try code catch {
    case e: Exception =>
      closeable.foreach { _.close }
      throw e
  }
}
