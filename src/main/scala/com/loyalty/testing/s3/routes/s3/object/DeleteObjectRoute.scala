package com.loyalty.testing.s3.routes.s3.`object`

import java.time.OffsetDateTime
import java.util.UUID

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.NoContent
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.loyalty.testing.s3.actor.SpawnBehavior.Command
import com.loyalty.testing.s3.actor.model.bucket.DeleteObjectWrapper
import com.loyalty.testing.s3.actor.model.{DeleteInfo, InvalidAccess, NoSuchBucketExists, NoSuchKeyExists}
import com.loyalty.testing.s3.repositories.model.ObjectKey
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.response.{InternalServiceException, NoSuchBucketException, NoSuchKeyException}
import com.loyalty.testing.s3.routes.CustomMarshallers
import com.loyalty.testing.s3.routes.s3._

import scala.util.{Failure, Success}

object DeleteObjectRoute extends CustomMarshallers {

  def apply(bucketName: String,
            key: String,
            objectIO: ObjectIO,
            database: NitriteDatabase)
           (implicit system: ActorSystem[Command],
            timeout: Timeout): Route =
    parameter("versionId".?) {
      maybeVersionId =>
        import system.executionContext
        val eventualEvent =
          for {
            actorRef <- spawnBucketBehavior(bucketName, objectIO, database)
            event <- askBucketBehavior(actorRef, replyTo => DeleteObjectWrapper(key, maybeVersionId, replyTo))
          } yield event
        onComplete(eventualEvent) {
          case Success(DeleteInfo(deleteMarker, version, maybeVersionId)) =>
            val objectKey = ObjectKey(
              id = UUID.randomUUID(),
              bucketName = bucketName,
              key = key,
              index = 0,
              version = version,
              versionId = maybeVersionId.getOrElse(""),
              eTag = "",
              contentMd5 = "",
              contentLength = 0,
              lastModifiedTime = OffsetDateTime.now,
              deleteMarker = Some(deleteMarker)
            )
            complete(HttpResponse(NoContent).withHeaders(createResponseHeaders(objectKey)))
          case Success(NoSuchBucketExists(_)) => complete(NoSuchBucketException(bucketName))
          case Success(NoSuchKeyExists) => complete(NoSuchKeyException(bucketName, key))
          case Success(InvalidAccess) =>
            system.log.warn("DeleteObjectRoute: invalid access to actor. bucket_name={}, key={}", bucketName, key)
            complete(InternalServiceException(s"$bucketName/$key"))
          case Success(event) =>
            system.log.warn("DeleteObjectRoute: invalid event received. event={}, bucket_name={}, key={}", event, bucketName, key)
            complete(InternalServiceException(s"$bucketName/$key"))
          case Failure(ex: Throwable) =>
            system.log.error(s"DeleteObjectRoute: Internal service error occurred, bucket_name=$bucketName, key=$key", ex)
            complete(InternalServiceException(s"$bucketName/$key"))
        }
    }
}
