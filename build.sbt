import uk.gov.hmrc.gitstamp.GitStampPlugin._
import sbt.Package.ManifestAttributes

lazy val extraBuildSettings = sys.props.get("buildNr").map(nr => Seq(packageOptions in (Compile, packageBin) += Package.ManifestAttributes("Build-number" -> nr))).getOrElse(Nil)

lazy val commonSettings = Seq(
    organization := "com.actian",
    version := "master",
    scalaVersion := "2.12.12",
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
    //Example configuration in order to talk to a local vector instance for testint purpose
    //javaOptions ++= Seq("-Dvector.host=localhost", "-Dvector.instance=VW", "-Dvector.database=testdb", "-Dvector.user=", "-Dvector.password="),
    scalacOptions ++= Seq( "-unchecked", "-deprecation" , "-feature"),
    // no scala version suffix on published artifact
    crossPaths := false
) ++ gitStampSettings ++ extraBuildSettings

lazy val commonDeps = Seq(
    "org.apache.spark" %% "spark-core" % "3.0.1" % "provided",
    "org.apache.spark" %% "spark-sql" % "3.0.1"  % "provided",
    "org.apache.spark" %% "spark-hive" % "3.0.1" % "provided",
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
    "com.typesafe.play" %% "play-json" % "2.9.1"
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
