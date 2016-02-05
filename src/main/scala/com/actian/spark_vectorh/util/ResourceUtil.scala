package com.actian.spark_vectorh.util

import scala.language.reflectiveCalls

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
  def closeResourceAfterUse[T, C <: { def close() }](closeable: C)(code: => T): T =
    try code finally {
      closeable.close()
    }

  /** Close a resource if an exception was thrown */
  def closeResourceOnFailure[T, C <: { def close() }](closeable: C)(code: => T): T =
    try code catch {
      case e: Exception =>
        closeable.close()
        throw e
    }
}
