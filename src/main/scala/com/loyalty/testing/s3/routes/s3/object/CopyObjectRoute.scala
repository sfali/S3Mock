package com.loyalty.testing.s3.routes.s3.`object`

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.NotFound
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.CopyBehavior.Copy
import com.loyalty.testing.s3.actor.SpawnBehavior.Command
import com.loyalty.testing.s3.actor.model.{InvalidAccess, NoSuchBucketExists, NoSuchKeyExists, ObjectInfo}
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.response.{CopyObjectResult, InternalServiceException, NoSuchBucketException, NoSuchKeyException}
import com.loyalty.testing.s3.routes.CustomMarshallers
import com.loyalty.testing.s3.routes.s3._

import scala.util.{Failure, Success}

object CopyObjectRoute extends CustomMarshallers {

  import directives._

  def apply(bucketName: String,
            key: String,
            objectIO: ObjectIO,
            database: NitriteDatabase)
           (implicit system: ActorSystem[Command],
            timeout: Timeout): Route =
    headerValueByType[`x-amz-copy-source`]() { source =>
      import system.executionContext

      val sourceBucketName = source.bucketName
      val sourceKey = source.key
      val maybeSourceVersionId = source.maybeVersionId
      val sourceBucketId = sourceBucketName.toUUID
      val targetBucketId = bucketName.toUUID

      val eventualEvent =
        for {
          actorRef <- spawnCopyBehavior(objectIO, database)
          event <- askCopyActor(actorRef, replyTo => Copy(sourceBucketName, sourceKey, bucketName, key,
            maybeSourceVersionId, replyTo))
        } yield event
      onComplete(eventualEvent) {
        case Success(ObjectInfo(objectKey)) if objectKey.deleteMarker.contains(true) =>
          complete(HttpResponse(NotFound).withHeaders(createResponseHeaders(objectKey)))
        case Success(ObjectInfo(objectKey)) =>
          complete(CopyObjectResult(objectKey.eTag, objectKey.actualVersionId, maybeSourceVersionId))
        case Success(NoSuchBucketExists(bucketId)) if bucketId == sourceBucketId =>
          complete(NoSuchBucketException(sourceBucketName))
        case Success(NoSuchBucketExists(bucketId)) if bucketId == targetBucketId =>
          complete(NoSuchBucketException(bucketName))
        case Success(NoSuchKeyExists(bucketName, key)) => complete(NoSuchKeyException(bucketName, key))
        case Success(InvalidAccess) =>
          system.log.warn("CopyObjectRoute: invalid access to actor. bucket_name={}, key={}", bucketName, key)
          complete(InternalServiceException(s"$bucketName/$key"))
        case Success(event) =>
          system.log.warn("CopyObjectRoute: invalid event received. event={}, bucket_name={}, key={}", event, bucketName, key)
          complete(InternalServiceException(s"$bucketName/$key"))
        case Failure(ex: Throwable) =>
          system.log.error(s"CopyObjectRoute: Internal service error occurred, bucket_name=$bucketName, key=$key", ex)
          complete(InternalServiceException(s"$bucketName/$key"))
      }
    }
}
