import sbt._

object Dependencies {

  object GroupIds {
    val Akka = "com.typesafe.akka"
    val LightbendAkka = "com.lightbend.akka"
    val Logback = "ch.qos.logback"
    val Circe = "io.circe"
    val Beachape = "com.beachape"
    val CodehausGroovy = "org.codehaus.groovy"
    val Heikoseeberger = "de.heikoseeberger"
    val TanUkkii007 = "com.github.TanUkkii007"
    val OrgScalaTest = "org.scalatest"
    val OrgSalacheck = "org.scalacheck"
    val OrgScalamock = "org.scalamock"
    val Aws = "software.amazon.awssdk"
    val Dizitart = "org.dizitart"
    val ScalaLangModules = "org.scala-lang.modules"
  }

  object ModuleIds {
    val AkkaActor = "akka-actor"
    val AkkaActorTyped = "akka-actor-typed"
    val AkkaStreams = "akka-stream"
    val AkkaStreamsTyped = "akka-stream-typed"
    val AkkaClusterShardingTyed = "akka-cluster-sharding-typed"
    val JacksonSerializer = "akka-serialization-jackson"
    val AkkaRemote = "akka-remote"
    val AkkaHttp = "akka-http"
    val AkkaHttpXml = "akka-http-xml"
    val AkkaHttpTestKit = "akka-http-testkit"
    val AkkaSl4j = "akka-slf4j"
    val DowningProvider = "akka-cluster-custom-downing"
    val LogbackClassic  = "logback-classic"
    val CirceCore = "circe-core"
    val CirceGeneric = "circe-generic"
    val CirceParser = "circe-parser"
    val CirceGenericExtras = "circe-generic-extras"
    val Enumeratum = "enumeratum"
    val EnumeratumCirce = "enumeratum-circe"
    val Groovy = "groovy"
    val AkkaHttpCirce = "akka-http-circe"
    val ScalaTest = "scalatest"
    val Scalacheck = "scalacheck"
    val ScalaMock = "scalamock"
    val AlpakkaSqs = "akka-stream-alpakka-sqs"
    val AlpakkaSns = "akka-stream-alpakka-sns"
    val AlpakkaS3 = "akka-stream-alpakka-s3"
    val Sts = "sts"
    val Sqs = "sqs"
    val Sns = "sns"
    val S3 = "s3"
    val AkkaStreamsTestKit = "akka-stream-testkit"
    val ActorTestKit = "akka-actor-testkit-typed"
    val ActorTypedTestKit = "akka-actor-testkit-typed"
    val Nitrite = "nitrite"
    val ScalaXml = "scala-xml"
  }

  object Versions {
    val Scala212 = "2.12.8"
    val Scala213 = "2.13.1"
    val AkkaVersion = "2.6.1"
    val AkkaHttpVersion = "10.1.11"
    val LightbendVersion = "1.1.2"
    val AwsSdk2Version = "2.10.35"
    val LogbackVersion = "1.2.3"
    val CirceVersion = "0.12.1"
    val EnumeratumVersion = "1.5.13"
    val EnumeratumCirceVersion = "1.5.21"
    val DowningProviderVersion = "0.0.13"
    val GroovyVersion = "2.5.4"
    val AkkaHttpCirceVersion = "1.29.1"
    val ScalaTestVersion = "3.2.0-M1"
    val ScalacheckVersion = "1.14.2"
    val ScalamockVersion = "4.4.0"
    val NitriteVersion = "3.3.0"
    val ScalaXmlVersion = "2.0.0-M1"
  }

  import GroupIds._
  import ModuleIds._
  import Versions._

  val AkkaCommon: Seq[ModuleID] = Seq(
    Akka            %% AkkaActorTyped               % AkkaVersion,
    Akka            %% AkkaStreams                  % AkkaVersion,
    Akka            %% AkkaStreamsTyped             % AkkaVersion,
    Akka            %% AkkaClusterShardingTyed      % AkkaVersion,
    Akka            %% AkkaRemote                   % AkkaVersion,
    Akka            %% JacksonSerializer            % AkkaVersion,
    Akka            %% AkkaSl4j                     % AkkaVersion,
    Logback         %  LogbackClassic               % LogbackVersion,
    CodehausGroovy  %  Groovy                       % GroovyVersion
  )

  val AkkaTest: Seq[ModuleID] = Seq(
    Akka            %% ActorTestKit                 % AkkaVersion                 % "test, it",
    Akka            %% ActorTypedTestKit            % AkkaVersion                 % "test, it",
    Akka            %% AkkaStreamsTestKit           % AkkaVersion                 % "test, it"
  )

  val AkkaHttps: Seq[ModuleID] = Seq(
    Akka            %% AkkaHttp                     % AkkaHttpVersion
      excludeAll ExclusionRule(organization = Akka, name = AkkaActor),
    Akka            %% AkkaHttpXml                  % AkkaHttpVersion
      excludeAll ExclusionRule(organization = Akka),
    Heikoseeberger  %% AkkaHttpCirce                % AkkaHttpCirceVersion
      excludeAll ExclusionRule(organization = Akka),
    Akka            %% AkkaHttpTestKit              % AkkaHttpVersion            % Test
      excludeAll ExclusionRule(organization = Akka)
  )

  val AwsCommonV1: Seq[ModuleID] = Seq(
    "com.amazonaws"  % "aws-java-sdk-sqs"  % "1.11.502",
    "com.amazonaws"  % "aws-java-sdk-sns"  % "1.11.502"
  )

  val S3IntegrationTesting: Seq[ModuleID] = Seq(
    Aws             %  Sts                          % AwsSdk2Version,
    Aws             %  S3                           % AwsSdk2Version   % IntegrationTest,
    LightbendAkka   %% AlpakkaS3                    % LightbendVersion % IntegrationTest
      excludeAll (ExclusionRule(organization = Akka), ExclusionRule(organization = Aws))
  )

  val AwsCommon: Seq[ModuleID] = Seq(
    LightbendAkka   %% AlpakkaSqs                   % LightbendVersion
      excludeAll (ExclusionRule(organization = Akka),
      ExclusionRule(organization = Aws)),
    LightbendAkka   %% AlpakkaSns                   % LightbendVersion
      excludeAll (ExclusionRule(organization = Akka),
      ExclusionRule(organization = Aws)),
    Aws             %  Sqs                          % AwsSdk2Version
      excludeAll(
      ExclusionRule(organization = Aws, name = "netty-nio-client"),
      ExclusionRule(organization = "io.netty")
    ),
    Aws             %  Sns                          % AwsSdk2Version
      excludeAll(
      ExclusionRule(organization = Aws, name = "netty-nio-client"),
      ExclusionRule(organization = "io.netty")
    )
  )

  val JsonAndEnum: Seq[ModuleID] = Seq(
    Circe           %% CirceCore                    % CirceVersion,
    Circe           %% CirceGeneric                 % CirceVersion,
    Circe           %% CirceParser                  % CirceVersion,
    Circe           %% CirceGenericExtras           % CirceVersion,
    Beachape        %% Enumeratum                   % EnumeratumVersion,
    Beachape        %% EnumeratumCirce              % EnumeratumCirceVersion
  )

  val Misc: Seq[ModuleID] = Seq(
    ScalaLangModules  %% ScalaXml                   % ScalaXmlVersion,
    Dizitart          %  Nitrite                    % NitriteVersion,
    TanUkkii007       %% DowningProvider            % DowningProviderVersion
  )

  val CommonTest: Seq[ModuleID] = Seq(
    OrgScalaTest    %% ScalaTest                    % ScalaTestVersion            % "test, it",
    OrgSalacheck    %% Scalacheck                   % ScalacheckVersion           % "test, it",
    OrgScalamock    %% ScalaMock                    % ScalamockVersion            % "test, it"
  )

}
