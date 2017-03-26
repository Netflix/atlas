import sbt._
import sbt.Keys._

object BuildSettings {

  val compilerFlags = Seq(
    "-deprecation",
    "-unchecked",
    "-Xexperimental",
    "-Xlint:_,-infer-any",
    "-feature",
    "-target:jvm-1.8")

  lazy val checkLicenseHeaders = taskKey[Unit]("Check the license headers for all source files.")
  lazy val formatLicenseHeaders = taskKey[Unit]("Fix the license headers for all source files.")

  lazy val storeBintrayCredentials = taskKey[Unit]("Store bintray credentials.")
  lazy val credentialsFile = Path.userHome / ".bintray" / ".credentials"

  lazy val baseSettings =
    sbtrelease.ReleasePlugin.releaseSettings ++
      GitVersion.settings ++
      scoverage.ScoverageSbtPlugin.projectSettings

  lazy val buildSettings = baseSettings ++ Seq(
    organization := "com.netflix.atlas_v1",
    scalaVersion := Dependencies.Versions.scala,
    scalacOptions ++= BuildSettings.compilerFlags,
    crossPaths := true,
    crossScalaVersions := Dependencies.Versions.crossScala,
    sourcesInBase := false,
    exportJars := true,   // Needed for one-jar, with multi-project
    externalResolvers := BuildSettings.resolvers,

    checkLicenseHeaders := License.checkLicenseHeaders(streams.value.log, sourceDirectory.value),
    formatLicenseHeaders := License.formatLicenseHeaders(streams.value.log, sourceDirectory.value),

    storeBintrayCredentials := {
      IO.write(
        credentialsFile,
        bintray.BintrayCredentials.api.template(Bintray.user, Bintray.pass))
    },

    packageOptions in (Compile, packageBin) += Package.ManifestAttributes(
      "Build-Date"   -> java.time.Instant.now().toString,
      "Build-Number" -> sys.env.getOrElse("TRAVIS_BUILD_NUMBER", "unknown"),
      "Commit"       -> sys.env.getOrElse("TRAVIS_COMMIT",       "unknown"))
  )

  val commonDeps = Seq(
    Dependencies.jsr305,
    Dependencies.scalaLogging,
    Dependencies.slf4jApi,
    Dependencies.spectatorApi,
    Dependencies.typesafeConfig,
    Dependencies.scalatest % "test")

  val resolvers = Seq(
    Resolver.mavenLocal,
    Resolver.jcenterRepo,
    "jfrog" at "http://oss.jfrog.org/oss-snapshot-local")

  // Don't create root.jar, from:
  // http://stackoverflow.com/questions/20747296/producing-no-artifact-for-root-project-with-package-under-multi-project-build-in
  lazy val noPackaging = Seq(
    Keys.`package` :=  file(""),
    packageBin in Global :=  file(""),
    packagedArtifacts :=  Map()
  )

  def profile: Project => Project = p => {
    bintrayProfile(p)
      .settings(buildSettings: _*)
      .settings(libraryDependencies ++= commonDeps)
  }

  // Disable bintray plugin when not running under CI. Avoids a bunch of warnings like:
  //
  // ```
  // Missing bintray credentials /Users/brharrington/.bintray/.credentials. Some bintray features depend on this.
  // [warn] Credentials file /Users/brharrington/.bintray/.credentials does not exist
  // ```
  def bintrayProfile(p: Project): Project = {
    if (credentialsFile.exists)
      p.settings(Bintray.settings)
    else
      p.disablePlugins(bintray.BintrayPlugin)
  }
}
