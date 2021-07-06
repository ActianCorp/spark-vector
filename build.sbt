import uk.gov.hmrc.gitstamp.GitStampPlugin._
import sbt.Package.ManifestAttributes

lazy val extraBuildSettings = sys.props.get("buildNr").map(nr => Seq(packageOptions in (Compile, packageBin) += Package.ManifestAttributes("Build-number" -> nr))).getOrElse(Nil)

lazy val commonSettings = Seq(
    organization := "com.actian",
    version := "2.1",
    scalaVersion := "2.11.12",
    libraryDependencies ++= commonDeps,
    fork in Test := true,
    test in assembly := {},
    scalacOptions ++= Seq( "-unchecked", "-deprecation" , "-feature"),
    //Example configuration in order to talk to a local vector instance for testing purposes
    /*
    javaOptions in Test ++= Seq(
        "-Dvector.host=localhost",
        "-Dvector.jdbcPort=27839",
        "-Dvector.database=testdb",
        "-Dvector.user=actian",
        "-Dvector.password=actian",
        "-Dprovider.sparkHome=/Users/fgropengieser/libs/spark-2.2.0-bin-hadoop2.7",
        "-Dprovider.jar=/Users/fgropengieser/project/spark_vector_2.1/provider/target/spark_vector_provider-assembly-2.1.jar",
        "-Dprovider.sparkInfoFile=/Users/fgropengieser/docker/shared_folders/spark-provider/spark_info_file"
    ),*/
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
