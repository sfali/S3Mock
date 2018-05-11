package com.loyalty.testing.s3.routes.s3.`object`

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.loyalty.testing.s3.repositories.Repository
import com.loyalty.testing.s3.response.{NoSuchBucketException, NoSuchKeyException}

import scala.util.Failure
import scala.util.Success

class DeleteObjectRoute private(log: LoggingAdapter, repository: Repository) {

  def route(bucketName: String, key: String): Route = {
    (delete & parameters("versionId".?)) { maybeVersionId =>
      onComplete(repository.deleteObject(bucketName, key)) {
        case Success(_) => complete(HttpResponse(NoContent))
        case Failure(ex: NoSuchBucketException) => complete(HttpResponse(NotFound, entity = ex.toXml.toString()))
        case Failure(ex: NoSuchKeyException) => complete(HttpResponse(NotFound, entity = ex.toXml.toString()))
        case Failure(ex) =>
          log.error(ex, "Error happened while deleting object {} in bucket: {}", key, bucketName)
          complete(HttpResponse(InternalServerError))
      }
    }
  }
}

object DeleteObjectRoute {
  def apply()(implicit log: LoggingAdapter, repository: Repository): DeleteObjectRoute =
    new DeleteObjectRoute(log, repository)
}
