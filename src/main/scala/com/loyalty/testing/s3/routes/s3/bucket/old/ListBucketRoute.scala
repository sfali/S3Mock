package com.loyalty.testing.s3.routes.s3.bucket.old

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
      (listType, delimiterParam, maxKeysParam, prefixParam) =>
        val maybePrefix = prefixParam match {
          case Some(value) => if (value == "") None else Some(value)
          case None => None
        }

        val maybeDelimiter = delimiterParam match {
          case Some(value) => if (value == "") None else Some(value)
          case None => None
        }
        log.debug("Delimiter: {}, Prefix: {}", maybeDelimiter, maybePrefix)
        if (listType != 2) complete(HttpResponse(BadRequest))
        else {
          val maxKeys = maxKeysParam.getOrElse(1000)
          val params = ListBucketParams(maxKeys, maybePrefix, maybeDelimiter)
          onComplete(repository.listBucket(bucketName, params)) {
            case Success(result) => complete(result)

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
