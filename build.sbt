
lazy val root = project.in(file("."))
  .configure(BuildSettings.profile)
  .aggregate(
    `atlas-akka`,
    `atlas-chart`,
    `atlas-config`,
    `atlas-core`,
    `atlas-jmh`,
    `atlas-json`,
    `atlas-lwcapi`,
    `atlas-module-akka`,
    `atlas-module-cloudwatch`,
    `atlas-module-lwcapi`,
    `atlas-module-webapi`,
    `atlas-poller`,
    `atlas-poller-cloudwatch`,
    `atlas-standalone`,
    `atlas-test`,
    `atlas-webapi`,
    `atlas-wiki`)
  .settings(BuildSettings.noPackaging: _*)

lazy val `atlas-akka` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-json`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.akkaActor,
    Dependencies.akkaSlf4j,
    Dependencies.iepService,
    Dependencies.spectatorSandbox,
    Dependencies.sprayCan,
    Dependencies.sprayRouting,
    Dependencies.typesafeConfig,
    Dependencies.akkaTestkit % "test",
    Dependencies.sprayTestkit % "test"
  ))

lazy val `atlas-chart` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-core`, `atlas-json`, `atlas-test` % "test")

lazy val `atlas-config` = project
  .configure(BuildSettings.profile)

lazy val `atlas-core` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-config`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.caffeine,
    Dependencies.equalsVerifier % "test",
    Dependencies.jol % "test"
  ))

lazy val `atlas-jmh` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-core`)
  .enablePlugins(pl.project13.scala.sbt.SbtJmh)

lazy val `atlas-json` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.jacksonCore2,
    Dependencies.jacksonJoda2,
    Dependencies.jacksonMapper2,
    Dependencies.jacksonScala2,
    Dependencies.jacksonSmile2,
    Dependencies.jodaConvert
  ))

lazy val `atlas-lwcapi` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-akka`, `atlas-core`, `atlas-json`, `atlas-test` % "test")
  .settings(libraryDependencies ++= Seq(
    Dependencies.spectatorSandbox,
    Dependencies.redisclient,
    Dependencies.akkaTestkit % "test",
    Dependencies.sprayTestkit % "test"
  ))

lazy val `atlas-module-akka` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-akka`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.guiceCore,
    Dependencies.guiceMulti,
    Dependencies.iepGuice
  ))

lazy val `atlas-module-cloudwatch` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-module-akka`, `atlas-poller-cloudwatch`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.guiceCore,
    Dependencies.guiceMulti,
    Dependencies.iepGuice,
    Dependencies.iepModuleAws

lazy val `atlas-module-lwcapi` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-lwcapi`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.guiceCore,
    Dependencies.iepGuice
  ))

lazy val `atlas-module-webapi` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-webapi`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.guiceCore,
    Dependencies.iepGuice
  ))

lazy val `atlas-poller` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-akka`, `atlas-core`, `atlas-webapi` % "test")
  .settings(libraryDependencies ++= Seq(
    Dependencies.sprayClient,
    Dependencies.akkaTestkit % "test",
    Dependencies.sprayTestkit % "test"
  ))

lazy val `atlas-poller-cloudwatch` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-core`, `atlas-poller`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.awsCloudWatch,
    Dependencies.frigga,
    Dependencies.iepService
  ))

lazy val `atlas-standalone` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-module-akka`, `atlas-module-lwcapi`, `atlas-module-webapi`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.iepGuice,
    Dependencies.guiceCore,
    Dependencies.guiceMulti,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,
    Dependencies.spectatorLog4j
  ))

lazy val `atlas-test` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-core`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.scalatest
  ))

lazy val `atlas-webapi` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-akka`, `atlas-chart`, `atlas-core`, `atlas-json`, `atlas-test` % "test")
  .settings(libraryDependencies ++= Seq(
    Dependencies.spectatorSandbox,
    Dependencies.akkaTestkit % "test",
    Dependencies.sprayTestkit % "test"
  ))

lazy val `atlas-wiki` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-core`, `atlas-webapi`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.scalaCompiler
  ))
