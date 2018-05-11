package com.loyalty.testing.s3.routes.s3.`object`

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.amazonaws.services.s3.Headers.{CONTENT_MD5, ETAG}
import com.loyalty.testing.s3.repositories.Repository
import com.loyalty.testing.s3.response.{NoSuchBucketException, NoSuchUploadException}

import scala.util.{Failure, Success}

class UploadMultipartRoute private(log: LoggingAdapter, repository: Repository) {

  def route(bucketName: String, key: String): Route =
    (put & extractRequest & parameters("partNumber".as[Int], "uploadId")) { (request, partNumber, uploadId) =>
      val eventualResult = repository.uploadMultipart(bucketName, key, partNumber, uploadId, request.entity.dataBytes)
      onComplete(eventualResult) {
        case Success(objectMeta) =>
          val putObjectResult = objectMeta.result
          var response = HttpResponse(OK,
            entity = HttpEntity(ContentType(MediaTypes.`application/xml`, HttpCharsets.`UTF-8`), ""))
            .withHeaders(RawHeader(CONTENT_MD5, putObjectResult.getContentMd5),
              RawHeader(ETAG, s""""${putObjectResult.getETag}""""))
          complete(response)
        case Failure(ex: NoSuchBucketException) => complete(HttpResponse(NotFound, entity = ex.toXml.toString()))
        case Failure(ex: NoSuchUploadException) => complete(HttpResponse(NotFound, entity = ex.toXml.toString()))
        case Failure(ex) =>
          log.error(ex, "Error happened while putting object {} in bucket: {}", key, bucketName)
          complete(HttpResponse(InternalServerError))
      }
    }
}

object UploadMultipartRoute {
  def apply()(implicit log: LoggingAdapter, repository: Repository): UploadMultipartRoute =
    new UploadMultipartRoute(log, repository)
}
