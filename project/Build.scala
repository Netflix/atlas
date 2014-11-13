import sbt._
import sbt.Keys._

object MainBuild extends Build {

  lazy val buildSettings = Seq(
    organization   := BuildSettings.organization,
    version        := BuildSettings.appVersion,
    scalaVersion   := BuildSettings.scalaVersion,
    crossPaths     := false,
    sourcesInBase  := false
  )

  lazy val root = project.in(file("."))
    .aggregate(core, json)

  lazy val core = project.in(file("atlas-core"))
    .settings(buildSettings: _*)
    .settings(libraryDependencies ++= commonDeps)

  lazy val json = project.in(file("atlas-json"))
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
