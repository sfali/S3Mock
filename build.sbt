import sbtrelease.ReleaseStateTransformations._

name := "s3mock"

lazy val commonSettings =
  Seq(organization := "com.loyalty.testing", scalaVersion := "2.12.6")

scalacOptions += "-Ypartial-unification"

resolvers ++= Seq(
  Resolver.bintrayRepo("hseeberger", "maven")
)

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(
    commonSettings,
    Defaults.itSettings,
    name := "s3mock",
    resolvers += Resolver.jcenterRepo,
    fork in Test := true,
    parallelExecution in IntegrationTest := false,
    libraryDependencies ++= {

      val akkaVersion = "2.5.12"
      val akkaHttpVersion = "10.1.1"
      val circeVersion = "0.9.0"
      val amazonawsVersion = "1.11.328"

      val akka = "com.typesafe.akka"
      val scalaTest = "org.scalatest"
      val scalaTestMock = "org.scalamock"
      val scalacheck = "org.scalacheck"
      val slf4j = "org.slf4j"
      val amazonaws = "com.amazonaws"
      val circe = "io.circe"
      val heikoseeberger = "de.heikoseeberger"
      val logback = "ch.qos.logback"
      val scalaLangModules = "org.scala-lang.modules"

      Seq(
        akka             %% "akka-http"                         % akkaHttpVersion,
        akka             %% "akka-http-xml"                     % akkaHttpVersion,
        akka 				     %% "akka-stream" 					            % akkaVersion,
        slf4j            %  "jcl-over-slf4j"                    % "1.7.25",
        logback          %  "logback-classic"                   % "1.2.3",
        circe            %% "circe-core"                        % circeVersion,
        circe            %% "circe-generic"                     % circeVersion,
        circe            %% "circe-parser"                      % circeVersion,
        circe            %% "circe-java8"                       % circeVersion,
        heikoseeberger   %% "akka-http-circe"                   % "1.18.0",
        scalaLangModules %% "scala-xml"                         % "1.0.6",
        amazonaws        % "aws-java-sdk-s3"                    % amazonawsVersion,
        amazonaws        % "aws-java-sdk-sqs"                   % amazonawsVersion,
        amazonaws        % "aws-java-sdk-sns"                   % amazonawsVersion,
        akka             %% "akka-testkit"                      % akkaVersion       % "it, test",
        scalaTest        %% "scalatest"                         % "3.0.4"           % "it, test",
        scalacheck       %% "scalacheck"                        % "1.13.5"          % "it, test",
        scalaTestMock    %% "scalamock-scalatest-support"       % "3.6.0"           % "it, test",
        "commons-io"     %  "commons-io"                        % "2.6"             % "it, test",
          akka 				     %% "akka-http-testkit" 				        % akkaHttpVersion   % Test,
        amazonaws        % "aws-java-sdk-sts"                   % amazonawsVersion  % IntegrationTest
      )
    }
  )
  .enablePlugins(JavaAppPackaging)

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  setNextVersion,
  commitNextVersion,
  pushChanges
)

dockerRepository := Some("loyaltyone")
dockerBaseImage := "loyaltyone/docker-slim-java-node:jre-8-node-8"
dockerEntrypoint := "/usr/local/bin/env-decrypt" +: dockerEntrypoint.value
