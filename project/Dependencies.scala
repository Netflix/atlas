import sbt._

// format: off

object Dependencies {
  object Versions {
    val pekko       = "1.1.2"
    val pekkoHttpV  = "1.1.0"
    val iep         = "5.0.28"
    val jackson     = "2.18.1"
    val log4j       = "2.24.1"
    val scala       = "2.13.15"
    val slf4j       = "2.0.16"
    val spectator   = "1.8.2"
    val spring      = "6.1.14"

    val crossScala  = Seq(scala, "3.5.1")
  }

  import Versions._

  val pekkoActor        = "org.apache.pekko" %% "pekko-actor" % pekko
  val pekkoHttp         = "org.apache.pekko" %% "pekko-http" % pekkoHttpV
  val pekkoHttpCore     = "org.apache.pekko" %% "pekko-http-core" % pekkoHttpV
  val pekkoHttpTestkit  = "org.apache.pekko" %% "pekko-http-testkit" % pekkoHttpV
  val pekkoSlf4j        = "org.apache.pekko" %% "pekko-slf4j" % pekko
  val pekkoStream       = "org.apache.pekko" %% "pekko-stream" % pekko
  val pekkoStreamTestkit= "org.apache.pekko" %% "pekko-stream-testkit" % pekko
  val pekkoTestkit      = "org.apache.pekko" %% "pekko-testkit" % pekko
  val caffeine          = "com.github.ben-manes.caffeine" % "caffeine" % "3.1.8"
  val datasketches      = "org.apache.datasketches" % "datasketches-java" % "6.1.1"
  val equalsVerifier    = "nl.jqno.equalsverifier" % "equalsverifier" % "3.17.3"
  val hikariCP          = "com.zaxxer" % "HikariCP" % "6.1.0"
  val iepLeaderApi      = "com.netflix.iep" % "iep-leader-api" % iep
  val iepLeaderDynamoDb = "com.netflix.iep" % "iep-leader-dynamodb" % iep
  val iepDynConfig      = "com.netflix.iep" % "iep-dynconfig" % iep
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
  val jol               = "org.openjdk.jol" % "jol-core" % "0.17"
  val jsr305            = "com.google.code.findbugs" % "jsr305" % "3.0.2"
  val log4jApi          = "org.apache.logging.log4j" % "log4j-api" % log4j
  val log4jCore         = "org.apache.logging.log4j" % "log4j-core" % log4j
  val log4jJcl          = "org.apache.logging.log4j" % "log4j-jcl" % log4j
  val log4jJul          = "org.apache.logging.log4j" % "log4j-jul" % log4j
  val log4jSlf4j        = "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4j
  val munit             = "org.scalameta" %% "munit" % "1.0.2"
  val postgres          = "org.postgresql" % "postgresql" % "42.7.4"
  val postgresEmbedded  = "io.zonky.test" % "embedded-postgres" % "2.0.7"
  val roaringBitmap     = "org.roaringbitmap" % "RoaringBitmap" % "1.3.0"
  val scalaCompat       = "org.scala-lang.modules" %% "scala-collection-compat" % "2.12.0"
  val scalaCompatJdk8   = "org.scala-lang.modules" %% "scala-java8-compat" % "1.0.2"
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
  val typesafeConfig    = "com.typesafe" % "config" % "1.4.3"
}

// format: on
