import sbtrelease.ReleaseStateTransformations._
import Dependencies._

name := "s3mock"
scalaVersion := Versions.Scala213
crossScalaVersions := Seq(Versions.Scala212, Versions.Scala213)

scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation", // Emit warning and location for usages of deprecated APIs.
  "-feature", // Emit warning and location for usages of features that should be imported explicitly.
  "-Ywarn-dead-code", // Warn when dead code is identified.
  "-Ywarn-unused:implicits", // Warn if an implicit parameter is unused.
  "-Ywarn-unused:imports", // Warn if an import selector is not referenced.
  "-Ywarn-unused:locals", // Warn if a local definition is unused.
  "-Ywarn-unused:params", // Warn if a value parameter is unused.
  "-Ywarn-unused:patvars", // Warn if a variable bound in a pattern is unused.
  "-Ywarn-unused:privates" // Warn if a private member is unused.
)

resolvers ++= Seq(
  Resolver.bintrayRepo("hseeberger", "maven"),
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots"),
  Resolver.mavenLocal,
  Resolver.jcenterRepo
)

lazy val root = project
  .in(file("."))
  .configs(IntegrationTest)
  .settings(
    organization += "com.loyalty.testing",
    Defaults.itSettings,
    name := "s3mock",
    fork in Test := true,
    parallelExecution in IntegrationTest := false,
    libraryDependencies ++= AkkaCommon ++ AkkaHttps ++ AwsCommonV1 ++ AwsCommon ++ JsonAndEnum ++ Misc ++ CommonTest ++
      AkkaTest ++ S3IntegrationTesting
  )
  .enablePlugins(JavaAppPackaging)

releaseIgnoreUntrackedFiles := true

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepTask(publish in Docker),
  setNextVersion,
  commitNextVersion,
  pushChanges
)

dockerRepository := Some("sfali23")
dockerExposedVolumes := Seq("/opt/docker/s3")
dockerBaseImage := "loyaltyone/docker-slim-java-node:jre8-node8-190509"
dockerEntrypoint := "/usr/local/bin/env-decrypt" +: dockerEntrypoint.value
