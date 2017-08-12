import sbt._
import sbt.Keys._
import bintray.BintrayPlugin._
import bintray.BintrayKeys._

object Bintray {

  lazy val now = System.currentTimeMillis
  lazy val isPullRequest = sys.env.getOrElse("TRAVIS_PULL_REQUEST", "false") != "false"

  private def get(k: String): String = {
    if (isPullRequest) s"dummy$k" else sys.env.getOrElse(s"bintray$k", s"missing$k")
  }


  lazy val user = get("User")
  lazy val pass = get("Key")

  lazy val settings: Seq[Def.Setting[_]] = bintraySettings ++ Seq(
    bintrayRepository := "maven",
    bintrayPackage := "atlas_v1",
    bintrayOrganization := Some("netflixoss"),
    bintrayReleaseOnPublish := false,
    licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.txt")),
    credentials += Credentials("Artifactory Realm", "oss.jfrog.org", user, pass),

    publishTo := {
      val defaultDestination = (publishTo in bintray value)
      if (isSnapshot.value)
        Some("OJO" at s"https://oss.jfrog.org/oss-snapshot-local;build.timestamp=$now/")
      else
        defaultDestination
    },

    pomExtra :=
      <url>https://github.com/netflix/atlas/wiki</url>
      <developers>
        <developer>
          <id>brharrington</id>
          <name>Brian Harrington</name>
          <email>brharrington@netflix.com</email>
        </developer>
      </developers>
  )
}
