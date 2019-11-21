package com.loyalty.testing.s3.routes.s3.bucket.old

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.{InternalServerError, NotFound, OK}
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.loyalty.testing.s3.repositories._
import com.loyalty.testing.s3.response.NoSuchBucketException

import scala.util.{Failure, Success}

class SetBucketVersioningRoute private(log: LoggingAdapter, repository: Repository) {

  def route(bucketName: String): Route =
    (put & extractRequest & parameter(Symbol("versioning"))) {
      (request, _) =>
        onComplete(repository.setBucketVersioning(bucketName, request.entity.dataBytes)) {
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