package com.actian.spark_vectorh.util

import scala.language.reflectiveCalls

object ResourceUtil {

  import resource._

  implicit class RichExtractableManagedResource[T](val res: ExtractableManagedResource[T]) extends AnyVal {
    def resolve(excMapper: Throwable => Throwable = identity): T = res.either.fold(e => throw excMapper(e.head), identity)
  }

  def closeResourceAfterUse[T, C <: { def close() }](closeable: C)(code: C => T): T =
    try code(closeable) finally {
      closeable.close()
    }

  def closeResourceOnFailure[T, C <: { def close() }](closeable: C)(code: => T): T =
    try code catch {
      case e: Exception =>
        closeable.close()
        throw e
    }
}
