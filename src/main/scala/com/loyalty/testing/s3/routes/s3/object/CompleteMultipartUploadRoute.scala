package com.loyalty.testing.s3.routes.s3.`object`

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.loyalty.testing.s3.notification.NotificationData
import com.loyalty.testing.s3.notification.actor.NotificationRouter
import com.loyalty.testing.s3.repositories.Repository
import com.loyalty.testing.s3.request.CompleteMultipartUpload
import com.loyalty.testing.s3.response.{InvalidPartException, InvalidPartOrderException, NoSuchBucketException, NoSuchUploadException}

import scala.util.{Failure, Success}

class CompleteMultipartUploadRoute private(notificationRouterRef: ActorRef, log: LoggingAdapter, repository: Repository) {

  def route(bucketName: String, key: String): Route = {
    (post & entity(as[String]) & parameter("uploadId")) { (requestXml, uploadId) =>
      val parts = CompleteMultipartUpload(Some(requestXml)).get
      onComplete(repository.completeMultipart(bucketName, key, uploadId, parts)) {
        case Success(response) =>
          val notificationData = NotificationData(response.bucketName, response.key, response.contentLength,
            response.eTag, "CompleteMultipartUpload", response.versionId)
          notificationRouterRef ! NotificationRouter.SendNotification(notificationData)

          val headers: List[HttpHeader] =
            response.versionId.fold(List[HttpHeader]()) {
              vId => RawHeader("x-amz-version-id", vId) :: Nil
            }
          val entity = HttpEntity(
            ContentType(MediaTypes.`application/xml`, HttpCharsets.`UTF-8`),
            response.toXml.toString())
          complete(HttpResponse(OK, entity = entity).withHeaders(headers))
        // complete(response)

        case Failure(ex: NoSuchBucketException) => complete(HttpResponse(NotFound, entity = ex.toXml.toString()))
        case Failure(ex: NoSuchUploadException) => complete(HttpResponse(NotFound, entity = ex.toXml.toString()))
        case Failure(ex: InvalidPartOrderException) => complete(HttpResponse(BadRequest, entity = ex.toXml.toString()))
        case Failure(ex: InvalidPartException) => complete(HttpResponse(BadRequest, entity = ex.toXml.toString()))
        case Failure(ex) =>
          log.error(ex, "Error happened while completing multipart request: {}:{}:{}", bucketName, key, uploadId)
          complete(HttpResponse(InternalServerError))
      }
    }
  }
}

object CompleteMultipartUploadRoute {
  def apply(notificationRouterRef: ActorRef)
           (implicit log: LoggingAdapter, repository: Repository): CompleteMultipartUploadRoute =
    new CompleteMultipartUploadRoute(notificationRouterRef, log, repository)
}
