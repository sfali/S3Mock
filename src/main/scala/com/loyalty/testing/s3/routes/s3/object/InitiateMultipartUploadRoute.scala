package com.loyalty.testing.s3.routes.s3.`object`

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.loyalty.testing.s3.repositories.Repository
import com.loyalty.testing.s3.routes.CustomMarshallers

import scala.util.{Failure, Success}

class InitiateMultipartUploadRoute private(log: LoggingAdapter, repository: Repository) extends CustomMarshallers {

  def route(bucketName: String, key: String): Route =
    (post & parameter('uploads)) { uploads =>
      onComplete(repository.initiateMultipartUpload(bucketName, key)) {
        case Success(result) => complete(result)
        case Failure(ex) => complete(HttpResponse(InternalServerError))
      }
    }
}

object InitiateMultipartUploadRoute {
  def apply()(implicit log: LoggingAdapter, repository: Repository): InitiateMultipartUploadRoute =
    new InitiateMultipartUploadRoute(log, repository)
}
