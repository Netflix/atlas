import sbt._
import sbt.Keys._
import xerial.sbt.Sonatype._
import xerial.sbt.Sonatype.SonatypeKeys._

object SonatypeSettings {

  private def get(k: String): String = {
    sys.env.getOrElse(s"NETFLIX_OSS_SONATYPE_$k", s"missing$k")
  }

  private lazy val user = get("USERNAME")
  private lazy val pass = get("PASSWORD")

  lazy val settings: Seq[Def.Setting[_]] = sonatypeSettings ++ Seq(
    sonatypeProfileName := "com.netflix",
    sonatypeProjectHosting := Some(GitHubHosting("Netflix", "atlas", "netflixoss@netflix.com")),

    publishMavenStyle := true,
    licenses += ("APL2" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt")),
    credentials += Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", user, pass),

    publishTo := sonatypePublishToBundle.value
  )
}
