import scala.collection.Seq

homepage in ThisBuild := Some(url("https://github.com/slamdata/quasar-destination-snowflake"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/slamdata/quasar-destination-snowflake"),
  "scm:git@github.com:slamdata/quasar-destination-snowflake.git"))

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  Test / packageBin / publishArtifact := true)

lazy val QuasarVersion = IO.read(file("./quasar-version")).trim
val DoobieVersion = "0.7.0"

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(core)
  .enablePlugins(AutomateHeaderPlugin)

lazy val core = project
  .in(file("core"))
  .settings(name := "quasar-destination-snowflake")
  .settings(
    performMavenCentralSync := false,
    publishAsOSSProject := true,
    quasarPluginName := "snowflake",
    quasarPluginQuasarVersion := QuasarVersion,
    quasarPluginDestinationFqcn := Some("quasar.destination.snowflake.SnowflakeDestinationModule$"),
    quasarPluginDependencies ++= Seq(
      "org.slf4s" %% "slf4s-api" % "1.7.25",
      "net.snowflake" % "snowflake-jdbc" % "3.10.1",
      "org.tpolecat" %% "doobie-core" % DoobieVersion,
      "org.tpolecat" %% "doobie-hikari" % DoobieVersion),
    libraryDependencies ++= Seq(
      "org.specs2" %% "specs2-core" % "4.8.3" % Test))
  .enablePlugins(AutomateHeaderPlugin, QuasarPlugin)
