package com.actian.spark_vectorh

import scala.collection.mutable.Stack
import scala.language.implicitConversions

import org.slf4j.Logger

case class ProfAccMap(accs: Map[String, ProfAcc], started: Stack[ProfAcc] = Stack.empty[ProfAcc])
case class ProfAcc(val name: String, var acc: Long = 0) {
  def print(log: Logger, msg: String): Unit = log.debug(f"$msg ${acc.toDouble / 1000000000}%.6fs")
  def print(log: Logger): Unit = print(log, s"#PROF $name =")
}

package object writer {
  def profile(acc: ProfAcc)(implicit pmap: ProfAccMap): Unit = {
    acc.acc -= System.nanoTime()
    pmap.started.push(acc)
  }

  def profileEnd(implicit pmap: ProfAccMap): Unit = {
    val last = pmap.started.pop()
    last.acc += System.nanoTime()
  }

  def profileInit(names: String*): ProfAccMap = ProfAccMap(names.map { case name => (name, ProfAcc(name)) }.toMap)
  def profilePrint(log: Logger)(implicit pmap: ProfAccMap): Unit = pmap.accs.map(_._2).foreach { _.print(log) }
  implicit def nameToAcc(name: String)(implicit pmap: ProfAccMap): ProfAcc = pmap.accs(name)
  val intSize = 4
}
