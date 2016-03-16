import sbt._
import sbt.Keys._

object MainBuild extends Build {

  lazy val baseSettings =
    sbtrelease.ReleasePlugin.releaseSettings ++
    GitVersion.settings ++
    Bintray.settings ++
    scoverage.ScoverageSbtPlugin.projectSettings

  lazy val buildSettings = baseSettings ++ Seq(
            organization := BuildSettings.organization,
            scalaVersion := Dependencies.Versions.scala,
           scalacOptions ++= BuildSettings.compilerFlags,
              crossPaths := false,
           sourcesInBase := false,
              exportJars := true,   // Needed for one-jar, with multi-project
       externalResolvers := BuildSettings.resolvers,
     checkLicenseHeaders := License.checkLicenseHeaders(streams.value.log, sourceDirectory.value),
    formatLicenseHeaders := License.formatLicenseHeaders(streams.value.log, sourceDirectory.value)
  )

  lazy val root = project.in(file("."))
    .aggregate(
      `atlas-akka`,
      `atlas-aws`,
      `atlas-chart`,
      `atlas-config`,
      `atlas-core`,
      `atlas-jmh`,
      `atlas-json`,
      `atlas-standalone`,
      `atlas-test`,
      `atlas-webapi`,
      `atlas-wiki`)
    .settings(buildSettings: _*)
    .settings(BuildSettings.noPackaging: _*)

  lazy val `atlas-akka` = project
    .dependsOn(`atlas-config`, `atlas-json`)
    .settings(buildSettings: _*)
    .settings(libraryDependencies ++= commonDeps)
    .settings(libraryDependencies ++= Seq(
      Dependencies.akkaActor,
      Dependencies.akkaSlf4j,
      Dependencies.iepService,
      Dependencies.spectatorSandbox,
      Dependencies.sprayCan,
      Dependencies.sprayRouting,
      Dependencies.typesafeConfig,
      Dependencies.akkaTestkit % "test",
      Dependencies.sprayTestkit % "test"
    ))

  lazy val `atlas-aws` = project
    .dependsOn(`atlas-config`)
    .settings(buildSettings: _*)
    .settings(libraryDependencies ++= commonDeps)
    .settings(libraryDependencies ++= Seq(
      Dependencies.awsCore,
      Dependencies.awsCloudWatch % "test",
      Dependencies.awsEC2 % "test",
      Dependencies.awsS3 % "test"
    ))

  lazy val `atlas-chart` = project
    .dependsOn(`atlas-core`, `atlas-json`, `atlas-test` % "test")
    .settings(buildSettings: _*)
    .settings(libraryDependencies ++= commonDeps)

  lazy val `atlas-config` = project
    .settings(buildSettings: _*)
    .settings(libraryDependencies ++= commonDeps)

  lazy val `atlas-core` = project
    .dependsOn(`atlas-config`)
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
    .enablePlugins(pl.project13.scala.sbt.SbtJmh)
    .settings(buildSettings: _*)
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

  lazy val `atlas-standalone` = project
    .dependsOn(`atlas-webapi`)
    .settings(buildSettings: _*)
    .settings(libraryDependencies ++= commonDeps)
    .settings(libraryDependencies ++= Seq(
      Dependencies.iepGovernator,
      Dependencies.guiceCore,
      Dependencies.guiceMulti,
      Dependencies.log4jApi,
      Dependencies.log4jCore,
      Dependencies.log4jSlf4j,
      Dependencies.spectatorLog4j
    ))

  lazy val `atlas-test` = project
    .dependsOn(`atlas-core`)
    .settings(buildSettings: _*)
    .settings(libraryDependencies ++= Seq(
      Dependencies.scalatest
    ))

  lazy val `atlas-webapi` = project
    .dependsOn(`atlas-akka`, `atlas-chart`, `atlas-core`, `atlas-json`, `atlas-test` % "test")
    .settings(buildSettings: _*)
    .settings(libraryDependencies ++= commonDeps)
    .settings(libraryDependencies ++= Seq(
      Dependencies.spectatorSandbox,
      Dependencies.akkaTestkit % "test",
      Dependencies.sprayTestkit % "test"
    ))

  lazy val `atlas-wiki` = project
    .dependsOn(`atlas-core`, `atlas-webapi`)
    .settings(buildSettings: _*)
    .settings(libraryDependencies ++= Seq(
      Dependencies.scalaCompiler
    ))

  val commonDeps = Seq(
    Dependencies.scalaLogging,
    Dependencies.scalaReflect,
    Dependencies.slf4jApi,
    Dependencies.spectatorApi,
    Dependencies.typesafeConfig,
    Dependencies.scalatest % "test")

  lazy val checkLicenseHeaders = taskKey[Unit]("Check the license headers for all source files.")
  lazy val formatLicenseHeaders = taskKey[Unit]("Fix the license headers for all source files.")
}
