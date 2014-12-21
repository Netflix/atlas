import sbt._
import sbt.Keys._

object BuildSettings {
  val organization = "com.netflix.atlas"

  val compilerFlags = Seq(
    "-deprecation",
    "-unchecked",
    "-Xlint:_,-infer-any",
    "-feature",
    "-target:jvm-1.7")

  // Don't create root.jar, from:
  // http://stackoverflow.com/questions/20747296/producing-no-artifact-for-root-project-with-package-under-multi-project-build-in
  lazy val noPackaging = Seq(
    Keys.`package` :=  file(""),
    packageBin in Global :=  file(""),
    packagedArtifacts :=  Map()
  )
}
