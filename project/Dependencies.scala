import sbt._

object Dependencies {
  object Versions {
    val akka       = "2.4.8"
    val aws        = "1.11.18"
    val iep        = "0.4.4"
    val guice      = "4.1.0"
    val jackson    = "2.7.4"
    val log4j      = "2.6.2"
    val scala      = "2.11.8"
    val slf4j      = "1.7.21"
    val spectator  = "0.40.0"
    val spray      = "1.3.3"
  }

  import Versions._

  val akkaActor       = "com.typesafe.akka" % "akka-actor_2.11" % akka
  val akkaSlf4j       = "com.typesafe.akka" % "akka-slf4j_2.11" % akka
  val akkaTestkit     = "com.typesafe.akka" % "akka-testkit_2.11" % akka
  val awsCloudWatch   = "com.amazonaws" % "aws-java-sdk-cloudwatch" % aws
  val awsCore         = "com.amazonaws" % "aws-java-sdk-core" % aws
  val awsEC2          = "com.amazonaws" % "aws-java-sdk-ec2" % aws
  val awsS3           = "com.amazonaws" % "aws-java-sdk-s3" % aws
  val caffeine        = "com.github.ben-manes.caffeine" % "caffeine" % "2.3.1"
  val equalsVerifier  = "nl.jqno.equalsverifier" % "equalsverifier" % "2.1.3"
  val guiceCore       = "com.google.inject" % "guice" % guice
  val guiceMulti      = "com.google.inject.extensions" % "guice-multibindings" % guice
  val iepGuice        = "com.netflix.iep" % "iep-guice" % iep
  val iepService      = "com.netflix.iep" % "iep-service" % iep
  val jacksonAnno2    = "com.fasterxml.jackson.core" % "jackson-annotations" % jackson
  val jacksonCore2    = "com.fasterxml.jackson.core" % "jackson-core" % jackson
  val jacksonJoda2    = "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % jackson
  val jacksonMapper2  = "com.fasterxml.jackson.core" % "jackson-databind" % jackson
  val jacksonScala2   = "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % jackson
  val jacksonSmile2   = "com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile" % jackson
  val jodaConvert     = "org.joda" % "joda-convert" % "1.8.1"
  val jol             = "org.openjdk.jol" % "jol-core" % "0.5"
  val log4jApi        = "org.apache.logging.log4j" % "log4j-api" % log4j
  val log4jCore       = "org.apache.logging.log4j" % "log4j-core" % log4j
  val log4jJcl        = "org.apache.logging.log4j" % "log4j-jcl" % log4j
  val log4jJul        = "org.apache.logging.log4j" % "log4j-jul" % log4j
  val log4jSlf4j      = "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4j
  val scalaCompiler   = "org.scala-lang" % "scala-compiler" % scala
  val scalaLibrary    = "org.scala-lang" % "scala-library" % scala
  val scalaLibraryAll = "org.scala-lang" % "scala-library-all" % scala
  val scalaLogging    = "com.typesafe.scala-logging" % "scala-logging_2.11" % "3.4.0"
  val scalaReflect    = "org.scala-lang" % "scala-reflect" % scala
  val scalatest       = "org.scalatest" % "scalatest_2.11" % "2.2.6"
  val slf4jApi        = "org.slf4j" % "slf4j-api" % slf4j
  val slf4jLog4j      = "org.slf4j" % "slf4j-log4j12" % slf4j
  val slf4jSimple     = "org.slf4j" % "slf4j-simple" % slf4j
  val spectatorApi    = "com.netflix.spectator" % "spectator-api" % spectator
  val spectatorLog4j  = "com.netflix.spectator" % "spectator-ext-log4j2" % spectator
  val spectatorM2     = "com.netflix.spectator" % "spectator-reg-metrics2" % spectator
  val spectatorSandbox= "com.netflix.spectator" % "spectator-ext-sandbox" % spectator
  val sprayCan        = "io.spray" % "spray-can_2.11" % spray
  val sprayClient     = "io.spray" % "spray-client_2.11" % spray
  val sprayRouting    = "io.spray" % "spray-routing_2.11" % spray
  val sprayTestkit    = "io.spray" % "spray-testkit_2.11" % spray
  val typesafeConfig  = "com.typesafe" % "config" % "1.3.0"
}
