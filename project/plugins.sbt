resolvers += Classpaths.sbtPluginReleases
resolvers += Resolver.url("hmrc-sbt-plugin-releases", url("https://dl.bintray.com/hmrc/sbt-plugin-releases"))(Resolver.ivyStylePatterns)
resolvers += Resolver.sonatypeRepo("snapshots")

addSbtPlugin("ch.epfl.scala" % "sbt-bloop" % "1.4.0-RC1-142-90456e06")

addSbtPlugin("uk.gov.hmrc" % "sbt-git-stamp" % "latest.integration")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "latest.integration")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "latest.integration")

addSbtPlugin("org.scalastyle" % "scalastyle-sbt-plugin" % "latest.integration")
