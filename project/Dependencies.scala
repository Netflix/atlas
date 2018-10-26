import sbt._

// format: off

object Dependencies {
  object Versions {
    val akka       = "2.5.17"
    val akkaHttpV  = "10.1.5"
    val aws        = "1.11.435"
    val iep        = "1.2.6"
    val guice      = "4.1.0"
    val jackson    = "2.9.7"
    val log4j      = "2.11.1"
    val scala      = "2.12.7"
    val slf4j      = "1.7.25"
    val spectator  = "0.79.0"

    val crossScala = Seq(scala, "2.11.12")
  }

  import Versions._

  val akkaActor       = "com.typesafe.akka" %% "akka-actor" % akka
  val akkaHttp        = "com.typesafe.akka" %% "akka-http" % akkaHttpV
  val akkaHttpCore    = "com.typesafe.akka" %% "akka-http-core" % akkaHttpV
  val akkaHttpTestkit = "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpV
  val akkaSlf4j       = "com.typesafe.akka" %% "akka-slf4j" % akka
  val akkaStream      = "com.typesafe.akka" %% "akka-stream" % akka
  val akkaTestkit     = "com.typesafe.akka" %% "akka-testkit" % akka
  val awsCloudWatch   = "com.amazonaws" % "aws-java-sdk-cloudwatch" % aws
  val awsCore         = "com.amazonaws" % "aws-java-sdk-core" % aws
  val awsEC2          = "com.amazonaws" % "aws-java-sdk-ec2" % aws
  val awsS3           = "com.amazonaws" % "aws-java-sdk-s3" % aws
  val caffeine        = "com.github.ben-manes.caffeine" % "caffeine" % "2.6.2"
  val equalsVerifier  = "nl.jqno.equalsverifier" % "equalsverifier" % "2.5.2"
  val frigga          = "com.netflix.frigga" % "frigga" % "0.18.0"
  val guiceCore       = "com.google.inject" % "guice" % guice
  val guiceMulti      = "com.google.inject.extensions" % "guice-multibindings" % guice
  val iepGuice        = "com.netflix.iep" % "iep-guice" % iep
  val iepModuleAtlas  = "com.netflix.iep" % "iep-module-atlas" % iep
  val iepModuleAws    = "com.netflix.iep" % "iep-module-aws" % iep
  val iepNflxEnv      = "com.netflix.iep" % "iep-nflxenv" % iep
  val iepService      = "com.netflix.iep" % "iep-service" % iep
  val jacksonAnno     = "com.fasterxml.jackson.core" % "jackson-annotations" % jackson
  val jacksonCore     = "com.fasterxml.jackson.core" % "jackson-core" % jackson
  val jacksonJava8    = "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % jackson
  val jacksonJoda     = "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % jackson
  val jacksonJsr310   = "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jackson
  val jacksonMapper   = "com.fasterxml.jackson.core" % "jackson-databind" % jackson
  val jacksonScala    = "com.fasterxml.jackson.module" %% "jackson-module-scala" % jackson
  val jacksonSmile    = "com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile" % jackson
  val jodaConvert     = "org.joda" % "joda-convert" % "2.1.1"
  val jol             = "org.openjdk.jol" % "jol-core" % "0.9"
  val jsr250          = "javax.annotation" % "jsr250-api" % "1.0"
  val jsr305          = "com.google.code.findbugs" % "jsr305" % "3.0.2"
  val log4jApi        = "org.apache.logging.log4j" % "log4j-api" % log4j
  val log4jCore       = "org.apache.logging.log4j" % "log4j-core" % log4j
  val log4jJcl        = "org.apache.logging.log4j" % "log4j-jcl" % log4j
  val log4jJul        = "org.apache.logging.log4j" % "log4j-jul" % log4j
  val log4jSlf4j      = "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4j
  val roaringBitmap   = "org.roaringbitmap" % "RoaringBitmap" % "0.7.17"
  val scalaCompiler   = "org.scala-lang" % "scala-compiler"
  val scalaLibrary    = "org.scala-lang" % "scala-library"
  val scalaLibraryAll = "org.scala-lang" % "scala-library-all"
  val scalaLogging    = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
  val scalaReflect    = "org.scala-lang" % "scala-reflect" % scala
  val scalatest       = "org.scalatest" %% "scalatest" % "3.0.5"
  val slf4jApi        = "org.slf4j" % "slf4j-api" % slf4j
  val slf4jLog4j      = "org.slf4j" % "slf4j-log4j12" % slf4j
  val slf4jSimple     = "org.slf4j" % "slf4j-simple" % slf4j
  val spectatorApi    = "com.netflix.spectator" % "spectator-api" % spectator
  val spectatorIpc    = "com.netflix.spectator" % "spectator-ext-ipc" % spectator
  val spectatorLog4j  = "com.netflix.spectator" % "spectator-ext-log4j2" % spectator
  val spectatorM2     = "com.netflix.spectator" % "spectator-reg-metrics2" % spectator
  val typesafeConfig  = "com.typesafe" % "config" % "1.3.3"
}

// format: on
