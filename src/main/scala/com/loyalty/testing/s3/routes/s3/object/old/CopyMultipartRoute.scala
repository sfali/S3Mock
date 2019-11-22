package com.loyalty.testing.s3.routes.s3.`object`.old

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.loyalty.testing.s3.repositories.Repository
import com.loyalty.testing.s3.response.{NoSuchBucketException, NoSuchKeyException, NoSuchUploadException}
import com.loyalty.testing.s3.routes.CustomMarshallers
import com.loyalty.testing.s3.routes.s3.`object`.directives

import scala.util.{Failure, Success}

class CopyMultipartRoute private(log: LoggingAdapter, repository: Repository) extends CustomMarshallers {

  import directives._

  def route(bucketName: String, key: String): Route = {
    (put & extractRequest & parameter("partNumber".as[Int]) & parameter("uploadId") &
      headerValueByType[`x-amz-copy-source`]() & optionalHeaderValueByType[`x-amz-copy-source-range`]()) {
      (_, partNumber, uploadId, source, sourceRange) =>
        val eventualResult = repository.copyMultipart(bucketName, key, partNumber, uploadId, source.bucketName,
          source.key, source.maybeVersionId, sourceRange.map(_.range))
        onComplete(eventualResult) {
          case Success(result) => complete(result)
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
          case Failure(ex: NoSuchUploadException) =>
            log.error(
              """NoSuchUploadException:
                |{}
              """.stripMargin, ex.toXml)
            complete(HttpResponse(NotFound, entity = ex.toXml.toString()))
          case Failure(ex) =>
            log.error(ex, "Error happened while copying object {} in bucket: {}", key, bucketName)
            complete(HttpResponse(InternalServerError))
        }
    }
  }
}

object CopyMultipartRoute {
  def apply()(implicit log: LoggingAdapter, repository: Repository): CopyMultipartRoute =
    new CopyMultipartRoute(log, repository)
}
