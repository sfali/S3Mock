package com.loyalty.testing.s3.routes.s3.`object`.old

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.notification.{NotificationData, OperationType}
import com.loyalty.testing.s3.notification.actor.NotificationRouter
import com.loyalty.testing.s3.repositories.Repository
import com.loyalty.testing.s3.response.NoSuchBucketException
import com.loyalty.testing.s3.routes.CustomMarshallers

import scala.util.{Failure, Success}

class PutObjectRoute private(notificationRouterRef: ActorRef,
                             log: LoggingAdapter,
                             repository: Repository)
  extends CustomMarshallers {

  def route(bucketName: String, key: String): Route = {
    put {
      extractRequest { request =>
        val eventualResult = repository.putObject(bucketName, key, request.entity.dataBytes)
        onComplete(eventualResult) {
          case Success(objectMeta) =>
            val putObjectResult = objectMeta.result
            val maybeVersionId = putObjectResult.maybeVersionId
            val notificationData = NotificationData(bucketName, key,
              putObjectResult.contentLength, putObjectResult.etag, OperationType.Put, maybeVersionId)
            notificationRouterRef ! NotificationRouter.SendNotification(notificationData)

            var response = HttpResponse(OK)
              .withHeaders(RawHeader(CONTENT_MD5, putObjectResult.contentMd5),
                RawHeader(ETAG, s""""${putObjectResult.etag}""""))
            response = maybeVersionId
              .map(versionId => response.addHeader(RawHeader("x-amz-version-id", versionId)))
              .getOrElse(response)
            complete(response)
          case Failure(ex: NoSuchBucketException) => complete(ex)
          case Failure(ex) =>
            log.error(ex, "Error happened while putting object {} in bucket: {}", key, bucketName)
            complete(HttpResponse(InternalServerError))
        }
      }
    }
  }

}

object PutObjectRoute {
  def apply(notificationRouterRef: ActorRef)(implicit log: LoggingAdapter, repository: Repository): PutObjectRoute =
    new PutObjectRoute(notificationRouterRef, log, repository)
}