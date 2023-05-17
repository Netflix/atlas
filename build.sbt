
lazy val atlas = project.in(file("."))
  .configure(BuildSettings.profile)
  .aggregate(
    `atlas-akka`,
    `atlas-akka-testkit`,
    `atlas-chart`,
    `atlas-core`,
    `atlas-eval`,
    `atlas-jmh`,
    `atlas-json`,
    `atlas-lwcapi`,
    `atlas-lwc-events`,
    `atlas-postgres`,
    `atlas-spring-akka`,
    `atlas-spring-eval`,
    `atlas-spring-lwc-events`,
    `atlas-spring-lwcapi`,
    `atlas-spring-webapi`,
    `atlas-standalone`,
    `atlas-webapi`)
  .settings(publish / skip := true)

lazy val `atlas-akka` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-json`, `atlas-akka-testkit` % "test")
  .settings(libraryDependencies ++= Seq(
    Dependencies.akkaActor,
    Dependencies.akkaSlf4j,
    Dependencies.akkaStream,
    Dependencies.iepDynConfig,
    Dependencies.iepService,
    Dependencies.spectatorIpc,
    Dependencies.akkaHttp,
    Dependencies.typesafeConfig,
    Dependencies.akkaHttpTestkit % "test",
    Dependencies.akkaStreamTestkit % "test",
    Dependencies.akkaTestkit % "test"
  ))

lazy val `atlas-akka-testkit` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
      Dependencies.akkaHttpTestkit,
      Dependencies.akkaStreamTestkit,
      Dependencies.akkaTestkit,
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
    Dependencies.datasketches,
    Dependencies.iepDynConfig,
    Dependencies.roaringBitmap,
    Dependencies.equalsVerifier % "test",
    Dependencies.jol % "test"
  ))

lazy val `atlas-eval` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-akka`, `atlas-chart`, `atlas-core`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.akkaHttpTestkit % "test",
    Dependencies.akkaStreamTestkit % "test",
    Dependencies.akkaTestkit % "test",
    Dependencies.equalsVerifier % "test"
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
    Dependencies.jacksonJava8,
    Dependencies.jacksonJsr310,
    Dependencies.jacksonMapper,
    Dependencies.jacksonScala,
    Dependencies.jacksonSmile
  ))

lazy val `atlas-lwcapi` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-akka`, `atlas-akka-testkit` % "test", `atlas-core`, `atlas-eval`, `atlas-json`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.iepDynConfig,
    Dependencies.akkaTestkit % "test",
    Dependencies.akkaHttpTestkit % "test",
    Dependencies.akkaStreamTestkit % "test"
  ))

lazy val `atlas-lwc-events` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-akka`, `atlas-core`, `atlas-json`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.iepDynConfig,
    Dependencies.spectatorAtlas
  ))

lazy val `atlas-postgres` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-core`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.postgres,
    Dependencies.postgresEmbedded % "test"
  ))

lazy val `atlas-spring-akka` = project
  .configure(BuildSettings.profile)
  .dependsOn(`atlas-akka`)
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
  .dependsOn(`atlas-spring-akka`, `atlas-lwcapi`)
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
  .dependsOn(`atlas-spring-akka`, `atlas-spring-lwcapi`, `atlas-spring-webapi`)
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
    `atlas-akka`,
    `atlas-akka-testkit` % "test",
    `atlas-chart`,
    `atlas-core`,
    `atlas-eval`,
    `atlas-json`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.akkaTestkit % "test",
    Dependencies.akkaHttpTestkit % "test",
    Dependencies.akkaStreamTestkit % "test"
  ))

