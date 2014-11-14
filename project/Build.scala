import sbt._
import sbt.Keys._
import com.typesafe.sbt.pgp.PgpKeys._

object MainBuild extends Build {

  lazy val additionalSettings = Sonatype.settings ++
    sbtrelease.ReleasePlugin.releaseSettings ++
    net.virtualvoid.sbt.graph.Plugin.graphSettings ++
    scoverage.ScoverageSbtPlugin.projectSettings

  lazy val buildSettings = Seq(
    organization      := BuildSettings.organization,
    scalaVersion      := BuildSettings.scalaVersion,
    crossPaths        := false,
    sourcesInBase     := false
  ) ++ additionalSettings

  lazy val root = project.in(file("."))
    .aggregate(`atlas-core`, `atlas-json`)
    .settings(buildSettings: _*)
    .settings(BuildSettings.noPackaging: _*)
    .settings(Sonatype.noPublishing: _*)

  lazy val `atlas-core` = project
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

  val commonDeps = Seq(Dependencies.scalatest % "test")
}
