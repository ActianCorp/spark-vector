organization := "com.actian"

name := "spark_vectorh"

version := "1.0-SNAPSHOT"

scalaVersion := "2.10.4"

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

scalacOptions ++= Seq( "-unchecked", "-deprecation" , "-feature")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.1"  % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.3" % "test"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.12.2" % "test"

libraryDependencies += "org.scalamock" %% "scalamock-scalatest-support" % "3.2.1" % "test"

libraryDependencies += "org.apache.sshd" % "sshd-core" % "0.11.0"

libraryDependencies += "com.jsuereth" %% "scala-arm" % "1.3"

fork in Test := true

test in assembly := {}

// no scala version suffix on published artifact
crossPaths := false

lazy val Deterministic = config("deterministic") extend(Test)

lazy val Local = config("local") extend(Test)

// FIXME simplify
lazy val root =
  Project("aurora", file("."))
  .configs(Deterministic)
  .settings(inConfig(Deterministic)(Defaults.testTasks): _*)
  .settings(testOptions in Deterministic := Seq(Tests.Argument("-l", "com.actian.dataflow.test.RandomizedTest")))
  .configs(Local)
  .settings(inConfig(Local)(Defaults.testTasks): _*)
  .settings(testOptions in Local := Seq(Tests.Argument("-l", "com.actian.dataflow.test.IntegrationTest")))

publishMavenStyle := true

val localMavenRepo = Resolver.file("file", new File(Path.userHome.absolutePath + "/.m2/repository"))

publishTo := Some(localMavenRepo)

compileOrder := CompileOrder.JavaThenScala
javacOptions ++= Seq("-source", "1.7", "-target", "1.7")
