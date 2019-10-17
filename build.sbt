import scala.collection.Seq

homepage in ThisBuild := Some(url("https://github.com/slamdata/quasar-destination-snowflake"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/slamdata/quasar-destination-snowflake"),
  "scm:git@github.com:slamdata/quasar-destination-snowflake.git"))

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  Test / packageBin / publishArtifact := true)

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
    publishAsOSSProject := false)
  .enablePlugins(AutomateHeaderPlugin)
