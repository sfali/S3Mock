package com.loyalty.testing.s3.routes.s3.`object`

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.StatusCodes.{NotFound, OK}
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.loyalty.testing.s3.actor.SpawnBehavior.Command
import com.loyalty.testing.s3.actor.model.bucket.GetObjectWrapper
import com.loyalty.testing.s3.actor.model._
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.response.{InternalServiceException, NoSuchBucketException, NoSuchKeyException}
import com.loyalty.testing.s3.routes.CustomMarshallers
import com.loyalty.testing.s3.routes.s3._

import scala.util.{Failure, Success}

object GetObjectRoute extends CustomMarshallers {

  def apply(bucketName: String,
            key: String,
            objectIO: ObjectIO,
            database: NitriteDatabase)
           (implicit system: ActorSystem[Command],
            timeout: Timeout): Route =
    (parameter("versionId".?) & optionalHeaderValue(extractRange)) {
      (maybeVersionId, maybeRange) =>
        import system.executionContext
        val eventualEvent =
          for {
            actorRef <- spawnBucketBehavior(bucketName, objectIO, database)
            event <- askBucketBehavior(actorRef, replyTo => GetObjectWrapper(key, maybeVersionId, maybeRange, replyTo))
          } yield event
        onComplete(eventualEvent) {
          case Success(ObjectContent(objectKey, content)) =>
            complete(HttpResponse(OK)
              .withEntity(HttpEntity(ContentTypes.`application/octet-stream`, objectKey.contentLength, content))
              .withHeaders(createResponseHeaders(objectKey)))
          case Success(ObjectInfo(objectKey)) => complete(HttpResponse(NotFound)
            .withHeaders(createResponseHeaders(objectKey)))
          case Success(NoSuchBucketExists(_)) => complete(NoSuchBucketException(bucketName))
          case Success(NoSuchKeyExists(bucketName, key)) => complete(NoSuchKeyException(bucketName, key))
          case Success(InvalidAccess) =>
            system.log.warn("GetObjectRoute: invalid access to actor. bucket_name={}, key={}", bucketName, key)
            complete(InternalServiceException(s"$bucketName/$key"))
          case Success(event) =>
            system.log.warn("GetObjectRoute: invalid event received. event={}, bucket_name={}, key={}", event, bucketName, key)
            complete(InternalServiceException(s"$bucketName/$key"))
          case Failure(ex: Throwable) =>
            system.log.error(s"GetObjectRoute: Internal service error occurred, bucket_name=$bucketName, key=$key", ex)
            complete(InternalServiceException(s"$bucketName/$key"))
        }
    }
}
