package com.loyalty.testing.s3.routes.s3.`object`.old

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.Source
import com.loyalty.testing.s3.repositories.Repository

class GetObjectMetadataRoute private(log: LoggingAdapter, repository: Repository) {

  def route(bucketName: String, key: String): Route = {
    (head & parameter("versionId".?)) { _ =>
      log.info("GetObjectMetadata, bucket_name={}, key={}", bucketName, key)
      val entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, 121, Source.empty)
      val httpResponse = HttpResponse(OK, entity = entity).addHeader(RawHeader("x-amz-version-id", "asdfqwer"))
      complete(httpResponse)
    }
  }
}

object GetObjectMetadataRoute {
  def apply()(implicit log: LoggingAdapter, repository: Repository): GetObjectMetadataRoute =
    new GetObjectMetadataRoute(log, repository)
}
