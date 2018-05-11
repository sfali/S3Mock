package com.loyalty.testing.s3.routes.s3.bucket

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.ContentTypes.`application/octet-stream`
import akka.http.scaladsl.model.{HttpEntity, HttpResponse}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.loyalty.testing.s3.repositories.Repository
import com.loyalty.testing.s3.request.CreateBucketConfiguration
import com.loyalty.testing.s3.response.BucketAlreadyExistsException
import com.loyalty.testing.s3.routes.CustomMarshallers

import scala.util.{Failure, Success}

class CreateBucketRoute private(log: LoggingAdapter, repository: Repository)  {

  def route(bucketName: String): Route = {
    (put & entity(as[Option[String]])) { maybeXml =>

      val bucketConfiguration = CreateBucketConfiguration(maybeXml)
      log.info("Got request to create bucket {} with bucket configuration {}", bucketName, bucketConfiguration)
      val futValue = repository.createBucket(bucketName, bucketConfiguration)

      onComplete(futValue) {
        case Success(response) =>
          complete(HttpResponse(OK).withHeaders(Location(s"/${response.bucketName}")))
        case Failure(ex: BucketAlreadyExistsException) =>
          val entity = HttpEntity(`application/octet-stream`, ex.toXml.toString().getBytes)
          complete(HttpResponse(BadRequest, entity = entity))
        case Failure(ex) =>
          log.error(ex, "Error happened while creating bucket: {}", bucketName)
          complete(HttpResponse(InternalServerError))
      }
    }
  }
}

object CreateBucketRoute {
  def apply()(implicit log: LoggingAdapter, repository: Repository): CreateBucketRoute =
    new CreateBucketRoute(log, repository)
}
