import sbt._
import sbt.Keys._
import bintray.BintrayPlugin._
import bintray.BintrayCredentials.api
import bintray.BintrayKeys._

object Bintray {

  lazy val now = System.currentTimeMillis
  lazy val isPullRequest = sys.env.getOrElse("TRAVIS_PULL_REQUEST", "false") != "false"

  private def get(k: String): String = {
    if (isPullRequest) s"dummy$k" else sys.env.getOrElse(s"bintray$k", s"missing$k")
  }

  lazy val user = get("User")
  lazy val pass = get("Key")

  lazy val storeBintrayCredentials = taskKey[Unit]("store bintray credentials")

  lazy val settings: Seq[Def.Setting[_]] = bintraySettings ++ Seq(
    bintrayRepository := "maven",
    bintrayPackage := "atlas_v1",
    bintrayOrganization := Some("netflixoss"),
    bintrayReleaseOnPublish := false,
    licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.txt")),
    credentials += Credentials("Artifactory Realm", "oss.jfrog.org", user, pass),

    publishTo := {
      if (isSnapshot.value)
        Some("OJO" at s"https://oss.jfrog.org/oss-snapshot-local;build.timestamp=$now/")
      else
        publishTo in bintray value
    },

    storeBintrayCredentials := {
      IO.write(bintrayCredentialsFile.value, api.template(user, pass))
    },

    pomExtra := (
      <url>https://github.com/netflix/atlas/wiki</url>
      <scm>
        <url>git@github.com:netflix/atlas.git</url>
        <connection>scm:git:git@github.com:netflix/atlas.git</connection>
      </scm>
      <developers>
        <developer>
          <id>brharrington</id>
          <name>Brian Harrington</name>
          <email>brharrington@netflix.com</email>
        </developer>
      </developers>
    )
  )
}
