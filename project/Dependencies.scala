import sbt._

object Dependencies {
  object Versions {
    val akka       = "2.3.8"
    val aws        = "1.9.13"
    val jackson    = "2.4.4"
    val lucene     = "4.10.2"
    val scala      = "2.11.5"
    val slf4j      = "1.7.9"
    val spectator  = "0.17-SNAPSHOT"
    val spray      = "1.3.2"
  }

  import Versions._

  val akkaActor       = "com.typesafe.akka" % "akka-actor_2.11" % akka
  val akkaSlf4j       = "com.typesafe.akka" % "akka-slf4j_2.11" % akka
  val akkaTestkit     = "com.typesafe.akka" % "akka-testkit_2.11" % akka
  val awsCloudWatch   = "com.amazonaws" % "aws-java-sdk-cloudwatch" % aws
  val awsCore         = "com.amazonaws" % "aws-java-sdk-core" % aws
  val awsEC2          = "com.amazonaws" % "aws-java-sdk-ec2" % aws
  val awsS3           = "com.amazonaws" % "aws-java-sdk-s3" % aws
  val equalsVerifier  = "nl.jqno.equalsverifier" % "equalsverifier" % "1.5.1"
  val eureka          = "com.netflix.eureka" % "eureka-client" % "1.1.142"
  val frigga          = "com.netflix.frigga" % "frigga" % "0.13"
  val guava           = "com.google.guava" % "guava" % "15.0"
  val hadoopCommon    = "org.apache.hadoop" % "hadoop-common" % "2.5.1"
  val jacksonAnno2    = "com.fasterxml.jackson.core" % "jackson-annotations" % jackson
  val jacksonCore2    = "com.fasterxml.jackson.core" % "jackson-core" % jackson
  val jacksonJoda2    = "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % jackson
  val jacksonMapper2  = "com.fasterxml.jackson.core" % "jackson-databind" % jackson
  val jacksonScala2   = "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % jackson
  val jacksonSmile2   = "com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile" % jackson
  val jna             = "net.java.dev.jna" % "jna" % "4.1.0"
  val jodaConvert     = "org.joda" % "joda-convert" % "1.7"
  val jodaTime        = "joda-time" % "joda-time" % "2.5"
  val jsr305          = "com.google.code.findbugs" % "jsr305" % "3.0.0"
  val log4j           = "log4j" % "log4j" % "1.2.17"
  val luceneCore      = "org.apache.lucene" % "lucene-core" % lucene
  val luceneQueries   = "org.apache.lucene" % "lucene-queries" % lucene
  val luceneRegex     = "org.apache.lucene" % "lucene-sandbox" % lucene
  val pegdown         = "org.pegdown" % "pegdown" % "1.4.2"
  val ribbonTransport = "com.netflix.ribbon" % "ribbon-transport" % "2.0-RC9"
  val rrd4j           = "com.netflix.rrd4j" % "rrd4j" % "2.2.5"
  val scalaLibrary    = "org.scala-lang" % "scala-library" % scala
  val scalaLibraryAll = "org.scala-lang" % "scala-library-all" % scala
  val scalaLogging    = "com.typesafe.scala-logging" % "scala-logging_2.11" % "3.1.0"
  val scalaParsec     = "org.scala-lang.modules" % "scala-parser-combinators_2.11" % "1.0.2"
  val scalaReflect    = "org.scala-lang" % "scala-reflect" % scala
  val scalatest       = "org.scalatest" % "scalatest_2.11" % "2.2.1"
  val servoCore       = "com.netflix.servo" % "servo-core" % "0.7.9"
  val slf4jApi        = "org.slf4j" % "slf4j-api" % slf4j
  val slf4jLog4j      = "org.slf4j" % "slf4j-log4j12" % slf4j
  val slf4jSimple     = "org.slf4j" % "slf4j-simple" % slf4j
  val snmp4j          = "org.snmp4j" % "snmp4j" % "2.3.0"
  val spectatorApi    = "com.netflix.spectator" % "spectator-api" % spectator
  val spectatorM2     = "com.netflix.spectator" % "spectator-reg-metrics2" % spectator
  val spectatorNflx   = "com.netflix.spectator" % "spectator-nflx" % spectator
  val sprayCan        = "io.spray" % "spray-can_2.11" % spray
  val sprayClient     = "io.spray" % "spray-client_2.11" % spray
  val sprayRouting    = "io.spray" % "spray-routing_2.11" % spray
  val sprayTestkit    = "io.spray" % "spray-testkit_2.11" % spray
  val trove           = "net.sf.trove4j" % "trove4j" % "3.0.3"
  val typesafeConfig  = "com.typesafe" % "config" % "1.2.1"
}
