
lazy val atlas = project.in(file("."))
  .configure(BuildSettings.profile)
  .aggregate(
    `atlas-pekko`,
    `atlas-pekko-testkit`,
    `atlas-chart`,
    `atlas-core`,
    `atlas-eval`,
    `atlas-jmh`,
    `atlas-json`,
    `atlas-lwcapi`,
    `atlas-lwc-events`,
    `atlas-postgres`,
    `atlas-spring-pekko`,
    `atlas-spring-eval`,
    `atlas-spring-lwc-events`,
    `atlas-spring-lwcapi`,
    `atlas-spring-webapi`,
    `atlas-standalone`,
    `atlas-webapi`)
  .settings(publish / skip := true)

lazy val `atlas-pekko` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-json`, `atlas-pekko-testkit` % "test")
  .settings(libraryDependencies ++= Seq(
    Dependencies.pekkoActor,
    Dependencies.pekkoSlf4j,
    Dependencies.pekkoStream,
    Dependencies.iepDynConfig,
    Dependencies.iepService,
    Dependencies.scalaCompatJdk8,
    Dependencies.spectatorIpc,
    Dependencies.pekkoHttp,
    Dependencies.typesafeConfig,
    Dependencies.pekkoHttpTestkit % "test",
    Dependencies.pekkoStreamTestkit % "test",
    Dependencies.pekkoTestkit % "test"
  ))

lazy val `atlas-pekko-testkit` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
      Dependencies.pekkoHttpTestkit,
      Dependencies.pekkoStreamTestkit,
      Dependencies.pekkoTestkit,
      Dependencies.munit
  ))

lazy val `atlas-chart` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-core`, `atlas-json`)

lazy val `atlas-core` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-json` % "test")
  .settings(libraryDependencies ++= Seq(
    Dependencies.caffeine,
    Dependencies.iepDynConfig,
    Dependencies.roaringBitmap,
    Dependencies.equalsVerifier % "test",
    Dependencies.jol % "test"
  ))

lazy val `atlas-eval` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-pekko`, `atlas-chart`, `atlas-core`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.equalsVerifier % "test",
    Dependencies.pekkoHttpTestkit % "test",
    Dependencies.pekkoStreamTestkit % "test",
    Dependencies.pekkoTestkit % "test"
  ))

lazy val `atlas-jmh` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-chart`, `atlas-core`, `atlas-eval`, `atlas-json`, `atlas-webapi`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.spectatorAtlas
  ))
  .enablePlugins(pl.project13.scala.sbt.SbtJmh)

lazy val `atlas-json` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.jacksonCore,
    Dependencies.jacksonMapper,
    Dependencies.jacksonScala,
    Dependencies.jacksonSmile
  ))

lazy val `atlas-lwcapi` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-pekko`, `atlas-pekko-testkit` % "test", `atlas-core`, `atlas-eval`, `atlas-json`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.iepDynConfig,
    Dependencies.spectatorAtlas,
    Dependencies.pekkoTestkit % "test",
    Dependencies.pekkoHttpTestkit % "test",
    Dependencies.pekkoStreamTestkit % "test"
  ))

lazy val `atlas-lwc-events` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-pekko`, `atlas-core`, `atlas-json`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.iepDynConfig,
    Dependencies.spectatorAtlas,
    Dependencies.otelSdk % "test",
    Dependencies.otelSdkTest % "test"
  ))

lazy val `atlas-postgres` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-core`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.hikariCP,
    Dependencies.iepService,
    Dependencies.postgres,
    Dependencies.postgresEmbedded % "test"
  ))

lazy val `atlas-spring-pekko` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-pekko`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.iepSpring
  ))

lazy val `atlas-spring-eval` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-eval`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.springContext
  ))

lazy val `atlas-spring-lwcapi` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-spring-pekko`, `atlas-lwcapi`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.iepSpring,
    Dependencies.springContext
  ))

lazy val `atlas-spring-lwc-events` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-lwc-events`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.iepSpring,
    Dependencies.springContext
  ))

lazy val `atlas-spring-webapi` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-webapi`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.iepSpring,
    Dependencies.springContext
  ))

lazy val `atlas-standalone` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-spring-pekko`, `atlas-spring-lwcapi`, `atlas-spring-webapi`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.iepSpring,
    Dependencies.iepSpringAtlas,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,
    Dependencies.spectatorLog4j
  ))

lazy val `atlas-webapi` = project
  .configure(BuildSettings.profile)
  .dependsOn(
    `atlas-pekko`,
    `atlas-pekko-testkit` % "test",
    `atlas-chart`,
    `atlas-core`,
    `atlas-eval`,
    `atlas-json`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.pekkoTestkit % "test",
    Dependencies.pekkoHttpTestkit % "test",
    Dependencies.pekkoStreamTestkit % "test"
  ))

