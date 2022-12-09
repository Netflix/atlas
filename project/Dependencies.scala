import sbt._

// format: off

object Dependencies {
  object Versions {
    // Do not upgrade Akka or Akka-HTTP versions, license has changed for newer
    // versions:
    //
    // - https://www.lightbend.com/blog/why-we-are-changing-the-license-for-akka
    // - https://github.com/akka/akka/pull/31561
    // - https://github.com/akka/akka-http/pull/4155
    val akka       = "2.6.20"
    val akkaHttpV  = "10.2.10"

    val iep        = "4.0.4"
    val jackson    = "2.14.1"
    val log4j      = "2.19.0"
    val scala      = "2.13.10"
    val slf4j      = "1.7.36"
    val spectator  = "1.4.2"
    val spring     = "5.3.24"

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
  val caffeine          = "com.github.ben-manes.caffeine" % "caffeine" % "2.9.3"
  val datasketches      = "org.apache.datasketches" % "datasketches-java" % "3.3.0"
  val equalsVerifier    = "nl.jqno.equalsverifier" % "equalsverifier" % "3.12.2"
  val iepLeaderApi      = "com.netflix.iep" % "iep-leader-api" % iep
  val iepLeaderDynamoDb = "com.netflix.iep" % "iep-leader-dynamodb" % iep
  val iepNflxEnv        = "com.netflix.iep" % "iep-nflxenv" % iep
  val iepService        = "com.netflix.iep" % "iep-service" % iep
  val iepSpring         = "com.netflix.iep" % "iep-spring" % iep
  val iepSpringAtlas    = "com.netflix.iep" % "iep-spring-atlas" % iep
  val jacksonAnno       = "com.fasterxml.jackson.core" % "jackson-annotations" % jackson
  val jacksonCore       = "com.fasterxml.jackson.core" % "jackson-core" % jackson
  val jacksonJava8      = "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % jackson
  val jacksonJsr310     = "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jackson
  val jacksonMapper     = "com.fasterxml.jackson.core" % "jackson-databind" % jackson
  val jacksonScala      = "com.fasterxml.jackson.module" %% "jackson-module-scala" % jackson
  val jacksonSmile      = "com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile" % jackson
  val jol               = "org.openjdk.jol" % "jol-core" % "0.16"
  val jsr305            = "com.google.code.findbugs" % "jsr305" % "3.0.2"
  val log4jApi          = "org.apache.logging.log4j" % "log4j-api" % log4j
  val log4jCore         = "org.apache.logging.log4j" % "log4j-core" % log4j
  val log4jJcl          = "org.apache.logging.log4j" % "log4j-jcl" % log4j
  val log4jJul          = "org.apache.logging.log4j" % "log4j-jul" % log4j
  val log4jSlf4j        = "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4j
  val munit             = "org.scalameta" %% "munit" % "0.7.29"
  val postgres          = "org.postgresql" % "postgresql" % "42.5.1"
  val postgresEmbedded  = "io.zonky.test" % "embedded-postgres" % "2.0.2"
  val roaringBitmap     = "org.roaringbitmap" % "RoaringBitmap" % "0.9.35"
  val scalaCompat       = "org.scala-lang.modules" %% "scala-collection-compat" % "2.9.0"
  val scalaCompiler     = "org.scala-lang" % "scala-compiler"
  val scalaLibrary      = "org.scala-lang" % "scala-library"
  val scalaLibraryAll   = "org.scala-lang" % "scala-library-all"
  val scalaLogging      = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
  val slf4jApi          = "org.slf4j" % "slf4j-api" % slf4j
  val slf4jLog4j        = "org.slf4j" % "slf4j-log4j12" % slf4j
  val slf4jSimple       = "org.slf4j" % "slf4j-simple" % slf4j
  val spectatorApi      = "com.netflix.spectator" % "spectator-api" % spectator
  val spectatorAtlas    = "com.netflix.spectator" % "spectator-reg-atlas" % spectator
  val spectatorIpc      = "com.netflix.spectator" % "spectator-ext-ipc" % spectator
  val spectatorLog4j    = "com.netflix.spectator" % "spectator-ext-log4j2" % spectator
  val spectatorM2       = "com.netflix.spectator" % "spectator-reg-metrics2" % spectator
  val springContext     = "org.springframework" % "spring-context" % spring
  val typesafeConfig    = "com.typesafe" % "config" % "1.4.2"
}

// format: on
