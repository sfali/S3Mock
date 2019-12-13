package com.loyalty.testing.s3.routes.s3.`object`

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.NotFound
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.CopyBehavior.CopyPart
import com.loyalty.testing.s3.actor.SpawnBehavior.Command
import com.loyalty.testing.s3.actor.model._
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.response.{CopyObjectResult, InternalServiceException, NoSuchBucketException, NoSuchKeyException}
import com.loyalty.testing.s3.routes.CustomMarshallers
import com.loyalty.testing.s3.routes.s3._
import com.loyalty.testing.s3.routes.s3.`object`.directives.{`x-amz-copy-source-range`, `x-amz-copy-source`}

import scala.util.{Failure, Success}

object CopyPartRoute extends CustomMarshallers {

  def apply(bucketName: String,
            key: String,
            objectIO: ObjectIO,
            database: NitriteDatabase)
           (implicit system: ActorSystem[Command],
            timeout: Timeout): Route =
    (parameter("partNumber".as[Int]) & parameter("uploadId") &
      headerValueByType[`x-amz-copy-source`](()) & optionalHeaderValueByType[`x-amz-copy-source-range`](())) {
      (partNumber, uploadId, copySource, maybeCopySourceRange) =>
        import system.executionContext

        val sourceBucketName = copySource.bucketName
        val sourceKey = copySource.key
        val maybeSourceVersionId = copySource.maybeVersionId
        val sourceBucketId = sourceBucketName.toUUID
        val targetBucketId = bucketName.toUUID
        val maybeRange = maybeCopySourceRange.map(_.range)

        val eventualEvent =
          for {
            actorRef <- spawnCopyBehavior(sourceBucketName, bucketName, objectIO, database)
            event <- askCopyActor(actorRef, replyTo => CopyPart(sourceBucketName, sourceKey, bucketName, key, uploadId,
              partNumber, maybeRange, maybeSourceVersionId, replyTo))
          } yield event
        onComplete(eventualEvent) {
          case Success(ObjectInfo(objectKey)) if objectKey.deleteMarker.contains(true) =>
            complete(HttpResponse(NotFound).withHeaders(createResponseHeaders(objectKey)))
          case Success(CopyPartInfo(uploadInfo, sourceVersionId)) =>
            val objectKey = uploadInfo.toObjectKey
            complete(CopyObjectResult(objectKey.eTag, objectKey.actualVersionId, sourceVersionId))
          case Success(ObjectInfo(_)) =>
            system.log.warn("CopyPartRoute: invalid access to actor. bucket_name={}, key={}", bucketName, key)
            complete(InternalServiceException(s"$bucketName/$key"))
          case Success(NoSuchBucketExists(bucketId)) if bucketId == sourceBucketId =>
            complete(NoSuchBucketException(sourceBucketName))
          case Success(NoSuchBucketExists(bucketId)) if bucketId == targetBucketId =>
            complete(NoSuchBucketException(bucketName))
          case Success(NoSuchKeyExists(bucketName, key)) => complete(NoSuchKeyException(bucketName, key))
          case Success(InvalidAccess) =>
            system.log.warn("CopyPartRoute: invalid access to actor. bucket_name={}, key={}", bucketName, key)
            complete(InternalServiceException(s"$bucketName/$key"))
          case Success(event) =>
            system.log.warn("CopyPartRoute: invalid event received. event={}, bucket_name={}, key={}", event, bucketName, key)
            complete(InternalServiceException(s"$bucketName/$key"))
          case Failure(ex: Throwable) =>
            system.log.error(s"CopyPartRoute: Internal service error occurred, bucket_name=$bucketName, key=$key", ex)
            complete(InternalServiceException(s"$bucketName/$key"))
        }
    }
}
