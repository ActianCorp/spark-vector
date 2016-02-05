package com.actian.spark_vectorh

import scala.collection.mutable.Stack
import scala.language.implicitConversions
import org.apache.spark.Logging

/** An accumulator for profiling, Contains a label and the number of milliseconds (`acc`) */
case class ProfAcc(val name: String, var acc: Long = 0) {
  def print(logFcn: (=> String) => Unit, msg: String): Unit = logFcn(f"$msg ${acc.toDouble / 1000000000}%.6fs")
  def print(logFcn: (=> String) => Unit): Unit = print(logFcn, s"#PROF $name =")
}

/** Contains all [[ProfAcc]]s defined and which of those are currently accumulating/measuring time (as a stack) */
case class ProfAccMap(accs: Map[String, ProfAcc], started: Stack[ProfAcc] = Stack.empty[ProfAcc])

/**
 * Trait to be used when profiling is needed. To profile a section of the code, the following steps should be followed:
 *  - Call [[profileInit]](<label_for_first_section>, <label_for_second_section>, ...) and store it into an implicit value
 *  - Use [[profile]](<section_name>) and [[profileEnd]] in a bracket opening/closing fashion, where the code between a
 *  [[profile]] call and its corresponding [[profileEnd]] will have its execution time measured and stored into its accumulator
 *  - Call [[profilePrint]] at the end to log the profiling information gathered
 */
trait Profiling {
  this: Logging =>
  /**
   * Start measuring time and record it in `acc`.
   *
   * @note This function should always be used in combination with [[profileEnd]] in a similar way with opening and closing
   * a sequence of brackets, where the code between a [[profile]] call and its corresponding [[profileEnd]] will have its
   * execution time measured and stored into `acc`
   */
  def profile(acc: ProfAcc)(implicit pmap: ProfAccMap): Unit = {
    acc.acc -= System.nanoTime()
    pmap.started.push(acc)
  }

  /** Finish profiling the current section of code, as determined by the most recent [[profile]] call */
  def profileEnd(implicit pmap: ProfAccMap): Unit = {
    val last = pmap.started.pop()
    last.acc += System.nanoTime()
  }

  /** Initialize profiling */
  def profileInit(names: String*): ProfAccMap = ProfAccMap(names.map { case name => (name, ProfAcc(name)) }.toMap)

  /** Print profile information using `log` */
  def profilePrint(implicit pmap: ProfAccMap): Unit = pmap.accs.map(_._2).foreach { _.print(logDebug) }

  implicit def nameToAcc(name: String)(implicit pmap: ProfAccMap): ProfAcc = pmap.accs(name)
}
