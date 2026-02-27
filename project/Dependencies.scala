import sbt.*

// format: off

object Dependencies {
  object Versions {
    val pekko       = "1.4.0"
    val pekkoHttpV  = "1.3.0"
    val iep         = "6.0.0"
    val jackson     = "3.0.4"
    val log4j       = "2.25.3"
    val otel        = "1.59.0"
    val scala       = "2.13.18"
    val slf4j       = "2.0.17"
    val spectator   = "1.9.4"
    val spring      = "7.0.4"

    val crossScala  = Seq(scala, "3.8.1")
  }

  import Versions.*

  val pekkoActor        = "org.apache.pekko" %% "pekko-actor" % pekko
  val pekkoHttp         = "org.apache.pekko" %% "pekko-http" % pekkoHttpV
  val pekkoHttpCore     = "org.apache.pekko" %% "pekko-http-core" % pekkoHttpV
  val pekkoHttpTestkit  = "org.apache.pekko" %% "pekko-http-testkit" % pekkoHttpV
  val pekkoSlf4j        = "org.apache.pekko" %% "pekko-slf4j" % pekko
  val pekkoStream       = "org.apache.pekko" %% "pekko-stream" % pekko
  val pekkoStreamTestkit= "org.apache.pekko" %% "pekko-stream-testkit" % pekko
  val pekkoTestkit      = "org.apache.pekko" %% "pekko-testkit" % pekko
  val caffeine          = "com.github.ben-manes.caffeine" % "caffeine" % "3.2.3"
  val equalsVerifier    = "nl.jqno.equalsverifier" % "equalsverifier" % "4.3.1"
  val hikariCP          = "com.zaxxer" % "HikariCP" % "7.0.2"
  val iepLeaderApi      = "com.netflix.iep" % "iep-leader-api" % iep
  val iepLeaderDynamoDb = "com.netflix.iep" % "iep-leader-dynamodb" % iep
  val iepDynConfig      = "com.netflix.iep" % "iep-dynconfig" % iep
  val iepService        = "com.netflix.iep" % "iep-service" % iep
  val iepSpring         = "com.netflix.iep" % "iep-spring" % iep
  val iepSpringAtlas    = "com.netflix.iep" % "iep-spring-atlas" % iep
  val jacksonCore       = "tools.jackson.core" % "jackson-core" % jackson
  val jacksonMapper     = "tools.jackson.core" % "jackson-databind" % jackson
  val jacksonScala      = "tools.jackson.module" %% "jackson-module-scala" % jackson
  val jacksonSmile      = "tools.jackson.dataformat" % "jackson-dataformat-smile" % jackson
  val jol               = "org.openjdk.jol" % "jol-core" % "0.17"
  val jsr305            = "com.google.code.findbugs" % "jsr305" % "3.0.2"
  val log4jApi          = "org.apache.logging.log4j" % "log4j-api" % log4j
  val log4jCore         = "org.apache.logging.log4j" % "log4j-core" % log4j
  val log4jJcl          = "org.apache.logging.log4j" % "log4j-jcl" % log4j
  val log4jJul          = "org.apache.logging.log4j" % "log4j-jul" % log4j
  val log4jSlf4j        = "org.apache.logging.log4j" % "log4j-slf4j2-impl" % log4j
  val munit             = "org.scalameta" %% "munit" % "1.2.2"
  val otelSdk           = "io.opentelemetry" % "opentelemetry-sdk" % otel
  val otelSdkTest       = "io.opentelemetry" % "opentelemetry-sdk-testing" % otel
  val postgres          = "org.postgresql" % "postgresql" % "42.7.9"
  val postgresEmbedded  = "io.zonky.test" % "embedded-postgres" % "2.2.0"
  val roaringBitmap     = "org.roaringbitmap" % "RoaringBitmap" % "1.6.12"
  val scalaCompat       = "org.scala-lang.modules" %% "scala-collection-compat" % "2.14.0"
  val scalaCompatJdk8   = "org.scala-lang.modules" %% "scala-java8-compat" % "1.0.2"
  val scalaCompiler     = "org.scala-lang" % "scala-compiler"
  val scalaLibrary      = "org.scala-lang" % "scala-library"
  val scalaLibraryAll   = "org.scala-lang" % "scala-library-all"
  val scalaLogging      = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.6"
  val slf4jApi          = "org.slf4j" % "slf4j-api" % slf4j
  val slf4jLog4j        = "org.slf4j" % "slf4j-log4j12" % slf4j
  val slf4jSimple       = "org.slf4j" % "slf4j-simple" % slf4j
  val spectatorApi      = "com.netflix.spectator" % "spectator-api" % spectator
  val spectatorAtlas    = "com.netflix.spectator" % "spectator-reg-atlas" % spectator
  val spectatorIpc      = "com.netflix.spectator" % "spectator-ext-ipc" % spectator
  val spectatorLog4j    = "com.netflix.spectator" % "spectator-ext-log4j2" % spectator
  val spectatorM2       = "com.netflix.spectator" % "spectator-reg-metrics2" % spectator
  val springContext     = "org.springframework" % "spring-context" % spring
  val typesafeConfig    = "com.typesafe" % "config" % "1.4.5"
}

// format: on
