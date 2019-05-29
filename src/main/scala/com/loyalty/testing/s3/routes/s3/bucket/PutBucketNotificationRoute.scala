package com.loyalty.testing.s3.routes.s3.bucket

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.{InternalServerError, OK}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.repositories.Repository
import com.loyalty.testing.s3.response.{InvalidNotificationConfigurationException, NoSuchBucketException}
import com.loyalty.testing.s3.routes.CustomMarshallers

import scala.util.{Failure, Success}

class PutBucketNotificationRoute private(log: LoggingAdapter, repository: Repository)
  extends CustomMarshallers {

  def route(bucketName: String, xml: String): Route =
    put {
      log.info("<<<<< {} >>>>>", xml)
      val notifications = parseNotificationConfiguration(bucketName, xml)
      val eventualResult = repository.putBucketNotification(bucketName, notifications)
      onComplete(eventualResult) {
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

object PutBucketNotificationRoute {
  def apply()(implicit log: LoggingAdapter, repository: Repository): PutBucketNotificationRoute =
    new PutBucketNotificationRoute(log, repository)
}
