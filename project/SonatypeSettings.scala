import sbt.*
import sbt.Keys.*

object SonatypeSettings {

  private def get(k: String): String = {
    sys.env.getOrElse(s"NETFLIX_OSS_SONATYPE_$k", s"missing$k")
  }

  private lazy val user = get("USERNAME")
  private lazy val pass = get("PASSWORD")

  lazy val settings: Seq[Def.Setting[_]] = Seq(
    organization := "com.netflix.atlas_v1",
    organizationName := "netflix",
    organizationHomepage := Some(url("https://github.com/Netflix")),
    homepage := Some(url("https://github.com/Netflix/atlas")),
    description := "In-memory time series database",

    scmInfo := Some(
      ScmInfo(
        url("https://github.com/Netflix/atlas"),
        "scm:git@github.com:Netflix/atlas.git"
      )
    ),

    developers := List(
      Developer(
        id = "netflixgithub",
        name = "Netflix Open Source Development",
        email = "netflixoss@netflix.com",
        url = url("https://github.com/Netflix")
      )
    ),

    publishMavenStyle := true,
    pomIncludeRepository := { _ => false },

    licenses += ("Apache 2" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt")),
    credentials += Credentials("Sonatype Nexus Repository Manager", "central.sonatype.com", user, pass),

    publishTo := {
      if (isSnapshot.value)
        Some(Resolver.sonatypeCentralSnapshots)
      else
        localStaging.value
    }
  )
}
