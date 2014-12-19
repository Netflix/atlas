import sbt._
import sbt.Keys._
import com.typesafe.sbt.SbtPgp
import java.io.File
import java.io.FileInputStream
import java.util.Properties
import sbtrelease.ReleasePlugin.ReleaseKeys._

object Sonatype {

  val keyDir   = new File(sys.env.getOrElse("GPG_KEY", "."))
  val propFile = new File(keyDir, "ci.properties")

  val username      = "ORG_GRADLE_PROJECT_sonatypeUsername"
  val password      = "ORG_GRADLE_PROJECT_sonatypePassword"
  val secretKeyRing = "ORG_GRADLE_PROJECT_signing.secretKeyRingFile"
  val keyPassword   = "ORG_GRADLE_PROJECT_signing.password"

  lazy val settings: Seq[Def.Setting[_]] = {
    if (!propFile.exists) Seq.empty else {
      val props = new Properties
      val input = new FileInputStream(propFile)
      try props.load(input) finally input.close()

      mavenSettings ++ SbtPgp.settings ++ Seq(
        SbtPgp.pgpPassphrase  := Some(props.getProperty(keyPassword).toArray),
        SbtPgp.pgpSecretRing  := new File(keyDir, s"secring.gpg"),
        credentials += Credentials(
          "Sonatype Nexus Repository Manager",
          "oss.sonatype.org",
          props.getProperty(username),
          props.getProperty(password))
      )
    }
  }

  lazy val mavenSettings = Seq(
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
    
    publishMavenStyle := true,
    
    publishArtifact in Test := false,
    
    publishArtifactsAction := SbtPgp.PgpKeys.publishSigned.value,

    pomIncludeRepository := { _ => false },
    
    pomExtra := (
      <url>https://github.com/netflix/atlas/wiki</url>
      <licenses>
        <license>
          <name>The Apache Software License, Version 2.0</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
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

  lazy val noPublishing = Seq(
    publish := (),
    publishLocal := (),
    publishTo := None
  )
}
