import sbt._
import sbt.Keys._

object BuildSettings {

  val compilerFlags = Seq(
    "-deprecation",
    "-unchecked",
    "-Xexperimental",
    "-Xlint:_,-infer-any",
    "-feature"
  )

  private val isRecentJdk = System.getProperty("java.specification.version").toDouble >= 11.0

  lazy val checkLicenseHeaders = taskKey[Unit]("Check the license headers for all source files.")
  lazy val formatLicenseHeaders = taskKey[Unit]("Fix the license headers for all source files.")

  lazy val baseSettings = GitVersion.settings

  lazy val buildSettings = baseSettings ++ Seq(
    organization := "com.netflix.atlas_v1",
    scalaVersion := Dependencies.Versions.scala,
    scalacOptions ++= {
      // -release option is not supported in scala 2.11
      val v = scalaVersion.value
      CrossVersion.partialVersion(v).map(_._2.toInt) match {
        case Some(v) if v > 11 && isRecentJdk => compilerFlags ++ Seq("-release", "8")
        case _                                => compilerFlags ++ Seq("-target:jvm-1.8")
      }
    },
    javacOptions ++= {
      if (isRecentJdk)
        Seq("--release", "8")
      else
        Seq("-source", "1.8", "-target", "1.8")
    },
    crossPaths := true,
    crossScalaVersions := Dependencies.Versions.crossScala,
    sourcesInBase := false,
    exportJars := true, // Needed for one-jar, with multi-project
    externalResolvers := BuildSettings.resolvers,
    // Evictions: https://github.com/sbt/sbt/issues/1636
    // Linting: https://github.com/sbt/sbt/pull/5153
    (update / evictionWarningOptions).withRank(KeyRanks.Invisible) := EvictionWarningOptions.empty,
    checkLicenseHeaders := License.checkLicenseHeaders(streams.value.log, sourceDirectory.value),
    formatLicenseHeaders := License.formatLicenseHeaders(streams.value.log, sourceDirectory.value),
    packageBin / packageOptions += Package.ManifestAttributes(
      "Build-Date"   -> java.time.Instant.now().toString,
      "Build-Number" -> sys.env.getOrElse("GITHUB_RUN_ID", "unknown"),
      "Commit"       -> sys.env.getOrElse("GITHUB_SHA", "unknown")
    ),
    // scaladoc crashes on jdk11 using `-release 8` with assertion failure:
    // type AnyRef in java.lang
    // https://github.com/scala/community-builds/issues/796#issuecomment-423395500
    publishArtifact in (Compile, packageDoc) := false,
    // Adds a `src/main/scala-2.13+` source directory for Scala 2.13 and newer
    // and a `src/main/scala-2.13-` source directory for Scala version older than 2.13
    unmanagedSourceDirectories in Compile += {
      val sourceDir = (sourceDirectory in Compile).value
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, n)) if n >= 13 => sourceDir / "scala-2.13+"
        case _                       => sourceDir / "scala-2.13-"
      }
    }
  )

  val commonDeps = Seq(
    Dependencies.jsr305,
    Dependencies.scalaLogging,
    Dependencies.slf4jApi,
    Dependencies.spectatorApi,
    Dependencies.typesafeConfig,
    Dependencies.scalatest % "test"
  )

  val resolvers = Seq(Resolver.mavenLocal, Resolver.mavenCentral) ++ Resolver.sonatypeOssRepos("snapshots")

  def profile: Project => Project = p => {
    p.settings(SonatypeSettings.settings)
      .settings(buildSettings: _*)
      .settings(libraryDependencies ++= commonDeps)
  }
}
