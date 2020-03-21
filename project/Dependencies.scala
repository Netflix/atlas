import sbt._

// format: off

object Dependencies {
  object Versions {
    val akka       = "2.5.29"
    val akkaHttpV  = "10.1.11"
    val aws        = "1.11.723"
    val iep        = "2.2.1"
    val guice      = "4.1.0"
    val jackson    = "2.10.2"
    val log4j      = "2.13.0"
    val scala      = "2.13.1"
    val slf4j      = "1.7.30"
    val spectator  = "0.103.0"

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
  val awsCloudWatch     = "com.amazonaws" % "aws-java-sdk-cloudwatch" % aws
  val awsCore           = "com.amazonaws" % "aws-java-sdk-core" % aws
  val awsEC2            = "com.amazonaws" % "aws-java-sdk-ec2" % aws
  val awsS3             = "com.amazonaws" % "aws-java-sdk-s3" % aws
  val caffeine          = "com.github.ben-manes.caffeine" % "caffeine" % "2.8.1"
  val equalsVerifier    = "nl.jqno.equalsverifier" % "equalsverifier" % "3.1.12"
  val frigga            = "com.netflix.frigga" % "frigga" % "0.20.0"
  val guiceCore         = "com.google.inject" % "guice" % guice
  val guiceMulti        = "com.google.inject.extensions" % "guice-multibindings" % guice
  val iepGuice          = "com.netflix.iep" % "iep-guice" % iep
  val iepLeaderApi      = "com.netflix.iep" % "iep-leader-api" % iep
  val iepLeaderDynamoDb = "com.netflix.iep" % "iep-leader-dynamodb" % iep
  val iepModuleAtlas    = "com.netflix.iep" % "iep-module-atlas" % iep
  val iepModuleAws      = "com.netflix.iep" % "iep-module-aws" % iep
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
  val jol               = "org.openjdk.jol" % "jol-core" % "0.10"
  val jsr250            = "javax.annotation" % "jsr250-api" % "1.0"
  val jsr305            = "com.google.code.findbugs" % "jsr305" % "3.0.2"
  val log4jApi          = "org.apache.logging.log4j" % "log4j-api" % log4j
  val log4jCore         = "org.apache.logging.log4j" % "log4j-core" % log4j
  val log4jJcl          = "org.apache.logging.log4j" % "log4j-jcl" % log4j
  val log4jJul          = "org.apache.logging.log4j" % "log4j-jul" % log4j
  val log4jSlf4j        = "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4j
  val roaringBitmap     = "org.roaringbitmap" % "RoaringBitmap" % "0.8.12"
  val scalaCompat       = "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.4"
  val scalaCompiler     = "org.scala-lang" % "scala-compiler"
  val scalaLibrary      = "org.scala-lang" % "scala-library"
  val scalaLibraryAll   = "org.scala-lang" % "scala-library-all"
  val scalaLogging      = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
  val scalaReflect      = "org.scala-lang" % "scala-reflect" % scala
  val scalatest         = "org.scalatest" %% "scalatest" % "3.1.0"
  val slf4jApi          = "org.slf4j" % "slf4j-api" % slf4j
  val slf4jLog4j        = "org.slf4j" % "slf4j-log4j12" % slf4j
  val slf4jSimple       = "org.slf4j" % "slf4j-simple" % slf4j
  val spectatorApi      = "com.netflix.spectator" % "spectator-api" % spectator
  val spectatorIpc      = "com.netflix.spectator" % "spectator-ext-ipc" % spectator
  val spectatorLog4j    = "com.netflix.spectator" % "spectator-ext-log4j2" % spectator
  val spectatorM2       = "com.netflix.spectator" % "spectator-reg-metrics2" % spectator
  val typesafeConfig    = "com.typesafe" % "config" % "1.4.0"
}

// format: on
