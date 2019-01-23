import uk.gov.hmrc.gitstamp.GitStampPlugin._
import sbt.Package.ManifestAttributes

lazy val extraBuildSettings = sys.props.get("buildNr").map(nr => Seq(packageOptions in (Compile, packageBin) += Package.ManifestAttributes("Build-number" -> nr))).getOrElse(Nil)

lazy val commonSettings = Seq(
    organization := "com.actian",
    version := "2.1",
    scalaVersion := "2.11.8",
    libraryDependencies ++= commonDeps,
    fork in Test := true,
    test in assembly := {},
    scalacOptions ++= Seq( "-unchecked", "-deprecation" , "-feature"),
    EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource,
    // no scala version suffix on published artifact
    crossPaths := false
) ++ gitStampSettings ++ extraBuildSettings

lazy val commonDeps = Seq(
    "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.2.0"  % "provided",
    "org.apache.spark" %% "spark-hive" % "2.2.0" % "provided",
    "org.scalatest" %% "scalatest" % "2.2.3" % "test",
    "org.scalacheck" %% "scalacheck" % "1.12.2" % "test",
    "org.scalamock" %% "scalamock-scalatest-support" % "3.2.1" % "test"
)

lazy val connectorDeps = Seq(
    "com.jsuereth" %% "scala-arm" % "2.0"
)

lazy val loaderDeps = Seq(
    "com.github.scopt" %% "scopt" % "3.3.0",
    "com.typesafe" % "config" % "1.3.0"
)

lazy val providerDeps = Seq(
    "com.typesafe.play" %% "play-json" % "2.3.10"
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
