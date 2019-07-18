package com.loyalty.testing.s3.routes.s3.bucket

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.{BadRequest, InternalServerError}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.loyalty.testing.s3.repositories.Repository
import com.loyalty.testing.s3.request.ListBucketParams
import com.loyalty.testing.s3.response.NoSuchBucketException
import com.loyalty.testing.s3.routes.CustomMarshallers

import scala.util.{Failure, Success}

class ListBucketRoute private(log: LoggingAdapter, repository: Repository)
  extends CustomMarshallers {

  def route(bucketName: String): Route =
    (get & parameter("list-type".as[Int]) & parameter("delimiter".?)
      & parameter("max-keys".as[Int].?) & parameter("prefix".?)) {
      (listType, maybeDelimiter, maybeMaxKeys, maybePrefix) =>
        if (listType != 2) {
          complete(HttpResponse(BadRequest))
        } else {
          val maxKeys = maybeMaxKeys.getOrElse(1000)
          val params = ListBucketParams(maxKeys, maybeDelimiter, maybePrefix)
          onComplete(repository.listBucket(bucketName, params)) {
            case Success(result) =>
              log.info("<<<<< {} >>>>>", result)
              complete(result)

            case Failure(ex: NoSuchBucketException) =>
              log.error(ex, ex.message)
              complete(ex)

            case Failure(ex) =>
              log.error(ex, "Error happened while getting list bucket: {}", bucketName)
              complete(HttpResponse(InternalServerError))
          }
        }
    }
}

object ListBucketRoute {
  def apply()(implicit log: LoggingAdapter, repository: Repository): ListBucketRoute =
    new ListBucketRoute(log, repository)
}
