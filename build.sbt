import uk.gov.hmrc.gitstamp.GitStampPlugin._
import sbt.Package.ManifestAttributes

val RELEASE_VERSION = "master"
val DEFAULT_SCALA_VERSION = "2.12.13"
val SPARK_VERSION = "3.1.1"

lazy val extraBuildSettings = sys.props
  .get("buildNr")
  .map(nr =>
    Seq(
      packageOptions in (Compile, packageBin) += Package.ManifestAttributes(
        "Build-number" -> nr
      )
    )
  )
  .getOrElse(Nil)

lazy val commonSettings = Seq(
  organization := "com.actian",
  version := RELEASE_VERSION,
  scalaVersion := DEFAULT_SCALA_VERSION,
  libraryDependencies ++= commonDeps,
  fork in Test := true,
  test in assembly := {},
  assemblyMergeStrategy in assembly := {
    case x if x.contains("module-info") =>
      MergeStrategy.first
    case x => {
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
    }
  },
  /*
  //Example configuration in order to talk to a local vector instance for testing purposes
  javaOptions in Test ++= Seq(
    "-Dvector.host=localhost",
    "-Dvector.jdbcPort=27839",
    "-Dvector.database=testdb",
    "-Dvector.user=actian",
    "-Dvector.password=actian",
    "-Dprovider.sparkHome=/Users/fgropengieser/libs/spark-3.1.1-bin-hadoop3.2",
    "-Dprovider.jar=/Users/fgropengieser/project/spark_vector_main/provider/target/spark_vector_provider-assembly-master.jar",
    "-Dprovider.sparkInfoFile=/Users/fgropengieser/docker/shared_folders/spark-provider/spark_info_file"
  ),*/
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
  // no scala version suffix on published artifact
  crossPaths := false
) ++ gitStampSettings ++ extraBuildSettings

lazy val commonDeps = Seq(
  "com.ingres.jdbc" % "iijdbc" % "10.2-4.1.10",
  "org.apache.spark" %% "spark-core" % SPARK_VERSION % "provided",
  "org.apache.spark" %% "spark-sql" % SPARK_VERSION % "provided",
  "org.apache.spark" %% "spark-hive" % SPARK_VERSION % "provided",
  "org.scalatest" %% "scalatest" % "3.2.2" % "test",
  "org.scalacheck" %% "scalacheck" % "1.14.1" % "test",
  "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0" % "test",
  "org.scalatestplus" %% "scalacheck-1-14" % "3.2.2.0" % "test"
)

lazy val connectorDeps = Seq(
  "com.jsuereth" %% "scala-arm" % "2.0"
)

lazy val loaderDeps = Seq(
  "com.github.scopt" %% "scopt" % "4.0.0",
  "com.typesafe" % "config" % "1.4.1"
)

lazy val providerDeps = Seq(
  "com.typesafe.play" %% "play-json" % "2.9.1",
  "io.skuber" %% "skuber" % "2.6.0"
)

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    name := "spark-vector",
    libraryDependencies ++= connectorDeps
  )
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "com.actian.spark_vector"
  )

lazy val loader = project
  .settings(commonSettings: _*)
  .settings(
    name := "spark_vector_loader",
    libraryDependencies ++= loaderDeps
  )
  .dependsOn(root)

lazy val provider = project
  .settings(commonSettings: _*)
  .settings(
    name := "spark_vector_provider",
    libraryDependencies ++= providerDeps
  )
  .dependsOn(loader)
