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

libraryDependencies += "com.jsuereth" %% "scala-arm" % "1.3"

fork in Test := true

test in assembly := {}

// no scala version suffix on published artifact
crossPaths := false

compileOrder := CompileOrder.JavaThenScala
javacOptions ++= Seq("-source", "1.7", "-target", "1.7")
