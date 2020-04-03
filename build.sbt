import uk.gov.hmrc.gitstamp.GitStampPlugin._
import sbt.Package.ManifestAttributes

lazy val extraBuildSettings = sys.props.get("buildNr").map(nr => Seq(packageOptions in (Compile, packageBin) += Package.ManifestAttributes("Build-number" -> nr))).getOrElse(Nil)

val sparkVersion="2.4.0"

lazy val commonSettings = Seq(
    organization := "com.actian",
    version := "2.1-SNAPSHOT",
    scalaVersion := "2.12.8",
    libraryDependencies ++= commonDeps,
    fork in Test := true,
    test in assembly := {},
    scalacOptions ++= Seq( "-unchecked", "-deprecation" , "-feature"),
    // no scala version suffix on published artifact
    crossPaths := false
) ++ gitStampSettings ++ extraBuildSettings

lazy val commonDeps = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion  % "provided",
    "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
    "org.scalacheck" %% "scalacheck" % "1.14.1" % "test",
    "org.scalamock" %% "scalamock" % "4.4.0" % "test",
    "org.scalactic" %% "scalactic" % "3.0.0",
    "org.scalatest" %% "scalatest" % "3.0.4" % "test",
    "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.10.3" % "provided",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.10.3" % "provided"
)

lazy val connectorDeps = Seq(
    "com.jsuereth" %% "scala-arm" % "2.0"
)

lazy val loaderDeps = Seq(
    "com.github.scopt" %% "scopt" % "3.7.1",
    "com.typesafe" % "config" % "1.4.0"
)

lazy val providerDeps = Seq(
    "com.typesafe.play" %% "play-json" % "2.8.1"
)

lazy val root = (project in file("."))
    .settings(commonSettings:_*)
    .settings(
        name := "spark-vector",
        libraryDependencies ++= connectorDeps
    )

lazy val loader = project
    .settings(commonSettings:_*)
    .settings(
        name := "spark_vector_loader",
        libraryDependencies ++= loaderDeps
    ).dependsOn(root)

lazy val provider = project
    .settings(commonSettings: _*)
    .settings(
        name := "spark_vector_provider",
        libraryDependencies ++= providerDeps
    ).dependsOn(loader)