package com.loyalty.testing.s3.routes.s3.bucket

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.{InternalServerError, OK}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.repositories.Repository
import com.loyalty.testing.s3.response.{InvalidNotificationConfigurationException, NoSuchBucketException}
import com.loyalty.testing.s3.routes.CustomMarshallers

import scala.util.{Failure, Success}

class SetBucketNotificationRoute private(log: LoggingAdapter, repository: Repository)
                                        (implicit mat: ActorMaterializer)
  extends CustomMarshallers {

  import mat.executionContext

  def route(bucketName: String): Route =
    (put & extractRequest & parameter('notification)) {
      (request, _) =>
      val eventualResult =
        request
          .entity
          .dataBytes
          .map(_.utf8String)
          .runWith(Sink.head)
          .map(s => parseNotificationConfiguration(bucketName, s))
          .flatMap {
            notifications =>
              repository.putBucketNotification(bucketName, notifications)
          }
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

object SetBucketNotificationRoute {
  def apply()(implicit log: LoggingAdapter, repository: Repository, mat: ActorMaterializer): SetBucketNotificationRoute =
    new SetBucketNotificationRoute(log, repository)
}
