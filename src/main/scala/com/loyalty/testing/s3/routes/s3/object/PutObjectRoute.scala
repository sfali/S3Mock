package com.loyalty.testing.s3.routes.s3.`object`

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.amazonaws.services.s3.Headers
import com.loyalty.testing.s3.repositories.Repository
import com.loyalty.testing.s3.response.NoSuchBucketException

import scala.util.{Failure, Success}

class PutObjectRoute private(log: LoggingAdapter, repository: Repository) {

  import Headers._

  def route(bucketName: String, key: String): Route = {
    put {
      extractRequest { request =>
        val eventualResult = repository.putObject(bucketName, key, request.entity.dataBytes)
        onComplete(eventualResult) {
          case Success(objectMeta) =>
            val result = objectMeta.result
            var response = HttpResponse(OK)
              .withHeaders(RawHeader(CONTENT_MD5, result.getContentMd5),
                RawHeader(ETAG, s""""${result.getETag}""""))
            response = Option(result.getVersionId)
              .map(versionId => response.addHeader(RawHeader("x-amz-version-id", versionId)))
              .getOrElse(response)
            complete(response)
          case Failure(ex: NoSuchBucketException) => complete(HttpResponse(NotFound, entity = ex.toXml.toString()))
          case Failure(ex) =>
            log.error(ex, "Error happened while putting object {} in bucket: {}", key, bucketName)
            complete(HttpResponse(InternalServerError))
        }
      }
    }
  }

}

object PutObjectRoute {
  def apply()(implicit log: LoggingAdapter, repository: Repository): PutObjectRoute =
    new PutObjectRoute(log, repository)
}