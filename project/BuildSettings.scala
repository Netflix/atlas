import sbt.Def
import sbt.*
import sbt.Keys.*

object BuildSettings {

  val compilerFlags: Seq[String] = Seq(
    "-deprecation",
    "-unchecked",
    "-Werror",
    "-feature",
    "-release", "17",
  )

  lazy val checkLicenseHeaders = taskKey[Unit]("Check the license headers for all source files.")
  lazy val formatLicenseHeaders = taskKey[Unit]("Fix the license headers for all source files.")

  lazy val baseSettings: Seq[Def.Setting[?]] = GitVersion.settings

  lazy val buildSettings: Seq[Def.Setting[?]] = baseSettings ++ Seq(
    organization := "com.netflix.atlas_v1",
    scalaVersion := Dependencies.Versions.scala,
    scalacOptions := {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, _)) => compilerFlags ++ Seq("-Xsource:3", "-Wunused")
        case _            => compilerFlags ++ Seq("-source", "3.3", "-Wunused:all")
      }
    },
    javacOptions ++= Seq("--release", "17"),
    crossPaths := true,
    crossScalaVersions := Dependencies.Versions.crossScala,
    sourcesInBase := false,
    exportJars := true, // Needed for one-jar, with multi-project
    externalResolvers := BuildSettings.resolvers,
    // Evictions: https://github.com/sbt/sbt/issues/1636
    // Linting: https://github.com/sbt/sbt/pull/5153
    (update / evictionWarningOptions).withRank(KeyRanks.Invisible) := EvictionWarningOptions.empty,
    checkLicenseHeaders := LicenseCheck.checkLicenseHeaders(streams.value.log, sourceDirectory.value),
    formatLicenseHeaders := LicenseCheck.formatLicenseHeaders(streams.value.log, sourceDirectory.value),
    packageBin / packageOptions += Package.ManifestAttributes(
      "Build-Date"   -> java.time.Instant.now().toString,
      "Build-Number" -> sys.env.getOrElse("GITHUB_RUN_ID", "unknown"),
      "Commit"       -> sys.env.getOrElse("GITHUB_SHA", "unknown")
    ),
    testFrameworks += new TestFramework("munit.Framework"),
    Test / parallelExecution := false
  )

  val commonDeps: Seq[ModuleID] = Seq(
    Dependencies.jsr305,
    Dependencies.scalaCompat,
    Dependencies.scalaLogging,
    Dependencies.slf4jApi,
    Dependencies.spectatorApi,
    Dependencies.typesafeConfig,
    Dependencies.munit % "test"
  )

  val resolvers: Seq[Resolver] = Seq(
    Resolver.mavenLocal,
    Resolver.mavenCentral,
    Resolver.sonatypeCentralSnapshots
  )

  def profile: Project => Project = p => {
    p.settings(SonatypeSettings.settings)
      .settings(buildSettings *)
      .settings(libraryDependencies ++= commonDeps)
  }
}
