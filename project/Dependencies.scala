import sbt._
import sbt.librarymanagement.DependencyBuilders.OrganizationArtifactName

// format: off

object Dependencies {
  object Versions {
    val akka       = "2.6.15"
    val akkaHttpV  = "10.2.4"
    val aws        = "2.16.92"
    val iep        = "3.0.2"
    val guice      = "5.0.1"
    val jackson    = "2.12.4"
    val log4j      = "2.14.1"
    val scala      = "2.13.6"
    val slf4j      = "1.7.31"
    val spectator  = "0.132.0"

    val crossScala = Seq(scala)
  }

  import Versions._

  val akkaActor         = "com.typesafe.akka" %% "akka-actor" % akka
  val akkaHttp          = "com.typesafe.akka" %% "akka-http" % akkaHttpV
  val akkaHttpCore      = "com.typesafe.akka" %% "akka-http-core" % akkaHttpV
  val akkaHttpTestkit   = "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpV
  val akkaSlf4j         = "com.typesafe.akka" %% "akka-slf4j" % akka
  val akkaStream        = "com.typesafe.akka" %% "akka-stream" % akka
  val akkaStreamTestkit = "com.typesafe.akka" %% "akka-stream-testkit" % akka
  val akkaTestkit       = "com.typesafe.akka" %% "akka-testkit" % akka
  val awsCloudWatch     = "software.amazon.awssdk" % "cloudwatch" % aws
  val caffeine          = "com.github.ben-manes.caffeine" % "caffeine" % "2.9.2"
  val datasketches      = "org.apache.datasketches" % "datasketches-java" % "2.0.0"
  val equalsVerifier    = "nl.jqno.equalsverifier" % "equalsverifier" % "3.7"
  val frigga            = "com.netflix.frigga" % "frigga" % "0.25.0"
  val guiceCoreBase     = "com.google.inject" % "guice"
  val guiceMultiBase    = "com.google.inject.extensions" % "guice-multibindings"
  val iepGuice          = "com.netflix.iep" % "iep-guice" % iep
  val iepLeaderApi      = "com.netflix.iep" % "iep-leader-api" % iep
  val iepLeaderDynamoDb = "com.netflix.iep" % "iep-leader-dynamodb" % iep
  val iepModuleAtlas    = "com.netflix.iep" % "iep-module-atlas" % iep
  val iepModuleAws      = "com.netflix.iep" % "iep-module-aws2" % iep
  val iepModuleLeader   = "com.netflix.iep" % "iep-module-leader" % iep
  val iepNflxEnv        = "com.netflix.iep" % "iep-nflxenv" % iep
  val iepService        = "com.netflix.iep" % "iep-service" % iep
  val jacksonAnno       = "com.fasterxml.jackson.core" % "jackson-annotations" % jackson
  val jacksonCore       = "com.fasterxml.jackson.core" % "jackson-core" % jackson
  val jacksonJava8      = "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % jackson
  val jacksonJsr310     = "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jackson
  val jacksonMapper     = "com.fasterxml.jackson.core" % "jackson-databind" % jackson
  val jacksonScala      = "com.fasterxml.jackson.module" %% "jackson-module-scala" % jackson
  val jacksonSmile      = "com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile" % jackson
  val jol               = "org.openjdk.jol" % "jol-core" % "0.16"
  val jsr250            = "javax.annotation" % "jsr250-api" % "1.0"
  val jsr305            = "com.google.code.findbugs" % "jsr305" % "3.0.2"
  val log4jApi          = "org.apache.logging.log4j" % "log4j-api" % log4j
  val log4jCore         = "org.apache.logging.log4j" % "log4j-core" % log4j
  val log4jJcl          = "org.apache.logging.log4j" % "log4j-jcl" % log4j
  val log4jJul          = "org.apache.logging.log4j" % "log4j-jul" % log4j
  val log4jSlf4j        = "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4j
  val roaringBitmap     = "org.roaringbitmap" % "RoaringBitmap" % "0.9.15"
  val scalaCompat       = "org.scala-lang.modules" %% "scala-collection-compat" % "2.5.0"
  val scalaCompiler     = "org.scala-lang" % "scala-compiler"
  val scalaLibrary      = "org.scala-lang" % "scala-library"
  val scalaLibraryAll   = "org.scala-lang" % "scala-library-all"
  val scalaLogging      = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4"
  val scalaReflect      = "org.scala-lang" % "scala-reflect" % scala
  val scalatest         = "org.scalatest" %% "scalatest" % "3.2.9"
  val slf4jApi          = "org.slf4j" % "slf4j-api" % slf4j
  val slf4jLog4j        = "org.slf4j" % "slf4j-log4j12" % slf4j
  val slf4jSimple       = "org.slf4j" % "slf4j-simple" % slf4j
  val spectatorApi      = "com.netflix.spectator" % "spectator-api" % spectator
  val spectatorIpc      = "com.netflix.spectator" % "spectator-ext-ipc" % spectator
  val spectatorLog4j    = "com.netflix.spectator" % "spectator-ext-log4j2" % spectator
  val spectatorM2       = "com.netflix.spectator" % "spectator-reg-metrics2" % spectator
  val typesafeConfig    = "com.typesafe" % "config" % "1.4.1"

  def isBeforeJava16: Boolean = {
    System.getProperty("java.specification.version").toDouble < 16
  }

  private def guiceDep(base: OrganizationArtifactName): ModuleID = {
    base % (if (isBeforeJava16) "4.1.0" else guice)
  }

  def guiceCore: ModuleID = guiceDep(guiceCoreBase)

  def guiceCoreAndMulti: Seq[ModuleID] = {
    if (isBeforeJava16)
      Seq(guiceDep(guiceCoreBase), guiceDep(guiceMultiBase))
    else
      Seq(guiceDep(guiceCoreBase))
  }
}

// format: on
