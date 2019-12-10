package com.loyalty.testing.s3.routes.s3.`object`

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.loyalty.testing.s3.actor.SpawnBehavior.Command
import com.loyalty.testing.s3.actor.model.bucket.CompleteUploadWrapper
import com.loyalty.testing.s3.actor.model._
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.request.CompleteMultipartUpload
import com.loyalty.testing.s3.response._
import com.loyalty.testing.s3.routes.CustomMarshallers
import com.loyalty.testing.s3.routes.s3._

import scala.util.{Failure, Success}

object CompleteUploadRoute extends CustomMarshallers {

  def apply(bucketName: String,
            key: String,
            objectIO: ObjectIO,
            database: NitriteDatabase)
           (implicit system: ActorSystem[Command],
            timeout: Timeout): Route =
    (extractRequest & parameter("uploadId")) { (request, uploadId) =>
      import system.executionContext
      val eventualEvent =
        extractRequestTo(request)
          .map(CompleteMultipartUpload.apply)
          .flatMap(execute(bucketName, key, uploadId, objectIO, database))
      onComplete(eventualEvent) {
        case Success(ObjectInfo(objectKey)) =>
          val result = CompleteMultipartUploadResult(bucketName, key, objectKey.eTag, objectKey.contentLength,
            objectKey.actualVersionId)
          complete(result)
        case Success(NoSuchBucketExists) => complete(NoSuchBucketException(bucketName))
        case Success(NoSuchUpload) => complete(NoSuchUploadException(bucketName, key))
        case Success(InvalidPartOrder) => complete(InvalidPartOrderException(bucketName, key))
        case Success(InvalidPart(partNumber)) => complete(InvalidPartException(bucketName, key, partNumber, uploadId))
        case Success(InvalidAccess) =>
          system.log.warn("CompleteUploadRoute: invalid access to actor. bucket_name={}, key={}", bucketName, key)
          complete(InternalServiceException(s"$bucketName/$key"))
        case Success(event) =>
          system.log.warn("CompleteUploadRoute: invalid event received. event={}, bucket_name={}, key={}", event, bucketName, key)
          complete(InternalServiceException(s"$bucketName/$key"))
        case Failure(ex: Throwable) =>
          system.log.error(s"CompleteUploadRoute: Internal service error occurred, bucket_name=$bucketName, key=$key", ex)
          complete(InternalServiceException(s"$bucketName/$key"))
      }
    }

  private def execute(bucketName: String,
                      key: String,
                      uploadId: String,
                      objectIO: ObjectIO,
                      database: NitriteDatabase)
                     (maybeCompleteMultipartUpload: Option[CompleteMultipartUpload])
                     (implicit system: ActorSystem[Command],
                      timeout: Timeout) = {
    import system.executionContext
    val parts = maybeCompleteMultipartUpload.map(_.parts).getOrElse(Nil)
    // system.log.info("Setting bucket versioning: {} on bucket: {}", maybeVersioningConfiguration, bucketName)
    for {
      actorRef <- spawnBucketBehavior(bucketName, objectIO, database)
      event <- askBucketBehavior(actorRef, replyTo => CompleteUploadWrapper(key, uploadId, parts, replyTo))
    } yield event
  }
}
