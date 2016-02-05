package com.actian.spark_vectorh.test.util

import org.scalacheck.Gen

object GenUtil {
  /** @note use with care! Excessive filtering may run out of candidates. */
  def distinct[T](size: Int)(gen: Gen[T]): Gen[List[T]] = {
    val initial = gen.map(List(_))
    (1 until size).foldLeft(initial) { (acc, cur) =>
      for {
        tail <- acc
        head <- gen if (!tail.contains(head))
      } yield head :: tail
    }
  }
}
