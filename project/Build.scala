import sbt._
import sbt.Keys._
import com.typesafe.sbt.pgp.PgpKeys._
import com.github.retronym.SbtOneJar._

object MainBuild extends Build {

  lazy val baseSettings = Sonatype.settings ++
    sbtrelease.ReleasePlugin.releaseSettings ++
    net.virtualvoid.sbt.graph.Plugin.graphSettings ++
    scoverage.ScoverageSbtPlugin.projectSettings

  lazy val buildSettings = baseSettings ++ Seq(
            organization := BuildSettings.organization,
            scalaVersion := BuildSettings.scalaVersion,
              crossPaths := false,
           sourcesInBase := false,
              exportJars := true,   // Needed for one-jar, with multi-project
               resolvers += Resolver.sonatypeRepo("snapshots"),
               resolvers += "rrd4j" at "https://raw.githubusercontent.com/brharrington/rrd4j/repo",
     checkLicenseHeaders := License.checkLicenseHeaders(streams.value.log, sourceDirectory.value),
    formatLicenseHeaders := License.formatLicenseHeaders(streams.value.log, sourceDirectory.value)
  )

  lazy val root = project.in(file("."))
    .aggregate(
      `atlas-akka`,
      `atlas-aws`,
      `atlas-chart`,
      `atlas-core`,
      `atlas-jmh`,
      `atlas-json`,
      `atlas-webapi`)
    .settings(buildSettings: _*)
    .settings(BuildSettings.noPackaging: _*)

  lazy val `atlas-akka` = project
    .dependsOn(`atlas-json`)
    .settings(buildSettings: _*)
    .settings(libraryDependencies ++= commonDeps)
    .settings(libraryDependencies ++= Seq(
      Dependencies.akkaActor,
      Dependencies.akkaSlf4j,
      Dependencies.sprayCan,
      Dependencies.sprayRouting,
      Dependencies.typesafeConfig,
      Dependencies.akkaTestkit % "test",
      Dependencies.sprayTestkit % "test"
    ))

  lazy val `atlas-aws` = project
    .settings(buildSettings: _*)
    .settings(libraryDependencies ++= commonDeps)
    .settings(libraryDependencies ++= Seq(
      Dependencies.awsCore,
      Dependencies.awsCloudWatch % "test",
      Dependencies.awsEC2 % "test",
      Dependencies.awsS3 % "test"
    ))

  lazy val `atlas-chart` = project
    .dependsOn(`atlas-core`, `atlas-json`)
    .settings(buildSettings: _*)
    .settings(libraryDependencies ++= commonDeps)
    .settings(libraryDependencies ++= Seq(
      Dependencies.rrd4j
    ))

  lazy val `atlas-core` = project
    .settings(buildSettings: _*)
    .settings(libraryDependencies ++= commonDeps)
    .settings(libraryDependencies ++= Seq(
      Dependencies.guava,
      Dependencies.jsr305,
      Dependencies.trove,
      Dependencies.equalsVerifier % "test"
    ))

  lazy val `atlas-jmh` = project
    .dependsOn(`atlas-core`)
    .settings(buildSettings: _*)
    .settings(pl.project13.scala.sbt.SbtJmh.jmhSettings: _*)
    .settings(libraryDependencies ++= commonDeps)

  lazy val `atlas-json` = project
    .settings(buildSettings: _*)
    .settings(libraryDependencies ++= commonDeps)
    .settings(libraryDependencies ++= Seq(
      Dependencies.jacksonCore2,
      Dependencies.jacksonJoda2,
      Dependencies.jacksonMapper2,
      Dependencies.jacksonScala2,
      Dependencies.jacksonSmile2,
      Dependencies.jodaConvert
    ))

  lazy val `atlas-webapi` = project
    .dependsOn(`atlas-akka`, `atlas-chart`, `atlas-core`, `atlas-json`)
    .settings(buildSettings: _*)
    .settings(oneJarSettings: _*)
    .settings(mainClass in oneJar := Some("com.netflix.atlas.webapi.Main"))
    .settings(libraryDependencies ++= commonDeps)
    .settings(libraryDependencies ++= Seq(
      Dependencies.slf4jSimple,
      Dependencies.akkaTestkit % "test",
      Dependencies.sprayTestkit % "test"
    ))

  val commonDeps = Seq(
    Dependencies.scalaLogging,
    Dependencies.slf4jApi,
    Dependencies.spectatorApi,
    Dependencies.typesafeConfig,
    Dependencies.scalatest % "test")

  lazy val checkLicenseHeaders = taskKey[Unit]("Check the license headers for all source files.")
  lazy val formatLicenseHeaders = taskKey[Unit]("Fix the license headers for all source files.")
}
