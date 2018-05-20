package com.loyalty.testing.s3.routes.s3.`object`

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.amazonaws.services.s3.Headers
import com.loyalty.testing.s3.repositories.Repository
import com.loyalty.testing.s3.response.{NoSuchBucketException, NoSuchKeyException}

import scala.util.Success
import scala.util.Failure

class GetObjectRoute private(log: LoggingAdapter, repository: Repository) {

  import Headers._

  def route(bucketName: String, key: String): Route = {
    (get & parameters("versionId".?) & optionalHeaderValueByType[Range]()) { (maybeVersionId, maybeRanges) =>
      val maybeRange: Option[ByteRange] = maybeRanges.map(_.ranges.head)
      onComplete(repository.getObject(bucketName, key, maybeVersionId, maybeRange)) {
        case Success(getObjectResponse) =>
          val eTagHeader = RawHeader(ETAG, s""""${getObjectResponse.eTag}"""")
          val maybeVersionHeader = getObjectResponse.maybeVersionId
            .map(versionId => RawHeader("x-amz-version-id", versionId))
          val defaultHeaders = List(eTagHeader)
          val headers = maybeVersionHeader.map(_ +: defaultHeaders).getOrElse(defaultHeaders)
          complete(HttpResponse(OK,
            entity = HttpEntity(
              ContentTypes.`application/octet-stream`,
              getObjectResponse.contentLength,
              getObjectResponse.content
            )
          ).withHeaders(headers))

        case Failure(ex: NoSuchBucketException) =>
          log.error(
            """NoSuchBucketException:
              |{}
            """.stripMargin, ex.toXml)
          complete(HttpResponse(NotFound, entity = ex.toXml.toString()))
        case Failure(ex: NoSuchKeyException) =>
          log.error(
            """NoSuchKeyException:
              |{}
            """.stripMargin, ex.toXml)
          complete(HttpResponse(NotFound, entity = ex.toXml.toString()))
        case Failure(ex) =>
          log.error(ex, "Error happened while getting object {} in bucket: {}", key, bucketName)
          complete(HttpResponse(InternalServerError))
      }
    }
  }

}

object GetObjectRoute {
  def apply()(implicit log: LoggingAdapter,
              repository: Repository): GetObjectRoute = new GetObjectRoute(log, repository)
}
