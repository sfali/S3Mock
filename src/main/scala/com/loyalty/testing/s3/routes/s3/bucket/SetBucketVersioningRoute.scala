package com.loyalty.testing.s3.routes.s3.bucket

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.{InternalServerError, NotFound, OK}
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.loyalty.testing.s3.repositories.Repository
import com.loyalty.testing.s3.request.{BucketVersioning, VersioningConfiguration}
import com.loyalty.testing.s3.response.NoSuchBucketException

import scala.util.{Failure, Success}

class SetBucketVersioningRoute private(log: LoggingAdapter, repository: Repository) {

  def route(bucketName: String, maybeXml: Option[String]): Route =
    put {
      val maybeVersioningConfiguration = VersioningConfiguration(maybeXml)
      val eventualResult = maybeVersioningConfiguration match {
        case Some(versioningConfiguration) =>
          log.info("Got request to set versioning on bucket {} with configuration: {}",
            bucketName, versioningConfiguration)
          repository.setBucketVersioning(bucketName, versioningConfiguration)
        case None =>
          log.warning(
            """
              |Got request to setBucketVersioning for bucket {} but no VersioningConfiguration
              |provided in the body, suspended VersioningConfiguration.
            """.stripMargin.replaceAll(System.lineSeparator(), ""), bucketName)
          repository.setBucketVersioning(bucketName, VersioningConfiguration(BucketVersioning.Suspended))
      }
      onComplete(eventualResult) {
        case Success(response) =>
          complete(HttpResponse(OK).withHeaders(Location(s"/${response.bucketName}")))
        case Failure(ex: NoSuchBucketException) => complete(HttpResponse(NotFound, entity = ex.toXml.toString()))
        case Failure(ex) =>
          log.error(ex, "Error happened while setting bucket versioning: {}", bucketName)
          complete(HttpResponse(InternalServerError))
      }
    }
}

object SetBucketVersioningRoute {
  def apply()(implicit log: LoggingAdapter, repository: Repository): SetBucketVersioningRoute =
    new SetBucketVersioningRoute(log, repository)
}