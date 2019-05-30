package com.loyalty.testing.s3.routes.s3.`object`

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.notification.NotificationData
import com.loyalty.testing.s3.notification.actor.NotificationRouter
import com.loyalty.testing.s3.repositories.Repository
import com.loyalty.testing.s3.response.{CopyObjectResult, NoSuchBucketException, ObjectMeta}
import com.loyalty.testing.s3.routes.CustomMarshallers

import scala.util.Failure

class CopyObjectRoute private(notificationRouterRef: ActorRef,
                              log: LoggingAdapter,
                              repository: Repository) extends CustomMarshallers {

  import directives._

  def route(bucketName: String, key: String): Route = {
    (put & headerValueByType[`x-amz-copy-source`]()) {
      source =>
        val sourceBucketName = source.bucketName
        val sourceKey = source.key
        val maybeSourceVersionId = source.maybeVersionId
        val eventualResult = repository.copyObject(bucketName, key, sourceBucketName, sourceKey, maybeSourceVersionId)
        onComplete(eventualResult) {
          case util.Success((objectMeta, copyObjectResult)) =>
            complete(toHttpResponse(objectMeta, copyObjectResult, bucketName, key, maybeSourceVersionId))

          case Failure(ex: NoSuchBucketException) => complete(HttpResponse(NotFound, entity = ex.toXml.toString()))
          case Failure(ex) =>
            log.error(ex, "Error happened while copying object {} in bucket: {} from object {}/{}", key,
              bucketName, sourceBucketName, sourceKey)
            complete(HttpResponse(InternalServerError))
        }
    }
  }

  private def toHttpResponse(objectMeta: ObjectMeta,
                             copyObjectResult: CopyObjectResult,
                             bucketName: String,
                             key: String,
                             maybeSourceVersionId: Option[String]) = {
    val putObjectResult = objectMeta.result

    val maybeVersionId = putObjectResult.maybeVersionId
    // send notification, if applicable
    val notificationData = NotificationData(bucketName, key,
      putObjectResult.contentLength, putObjectResult.etag, "Copy", maybeVersionId)
    notificationRouterRef ! NotificationRouter.SendNotification(notificationData)

    val headers = Nil +
      (ETAG, s""""${putObjectResult.etag}"""") +
      (CONTENT_MD5, putObjectResult.contentMd5) +
      ("x-amz-copy-source-version-id", maybeVersionId) +
      ("x-amz-version-id", maybeVersionId)

    val entity = HttpEntity(
      ContentType(MediaTypes.`application/xml`, HttpCharsets.`UTF-8`),
      copyObjectResult.toXml.toString())

    HttpResponse(OK, headers = headers, entity = entity)
  }
}

object CopyObjectRoute {
  def apply(notificationRouterRef: ActorRef,
            log: LoggingAdapter,
            repository: Repository): CopyObjectRoute =
    new CopyObjectRoute(notificationRouterRef, log, repository)
}
