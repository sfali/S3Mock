package com.loyalty.testing.s3.routes.s3.bucket.old

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.{InternalServerError, OK}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.loyalty.testing.s3.repositories.Repository
import com.loyalty.testing.s3.response.{InvalidNotificationConfigurationException, NoSuchBucketException}
import com.loyalty.testing.s3.routes.CustomMarshallers

import scala.util.{Failure, Success}

class SetBucketNotificationRoute private(log: LoggingAdapter, repository: Repository)
  extends CustomMarshallers {

  def route(bucketName: String): Route =
    (put & extractRequest & parameter(Symbol("notification"))) {
      (request, _) =>
        onComplete(repository.setBucketNotification(bucketName, request.entity.dataBytes)) {
          case Success(_) => complete(HttpResponse(OK))

          case Failure(ex: InvalidNotificationConfigurationException) =>
            log.error(ex, ex.message)
            complete(ex)

          case Failure(ex: NoSuchBucketException) =>
            log.error(ex, ex.message)
            complete(ex)

          case Failure(ex) =>
            log.error(ex, "Error happened while setting bucket notification: {}", bucketName)
            complete(HttpResponse(InternalServerError))
        }
    }
}

object SetBucketNotificationRoute {
  def apply()(implicit log: LoggingAdapter, repository: Repository): SetBucketNotificationRoute =
    new SetBucketNotificationRoute(log, repository)
}
