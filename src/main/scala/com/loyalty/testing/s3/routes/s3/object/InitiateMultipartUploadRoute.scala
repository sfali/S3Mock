package com.loyalty.testing.s3.routes.s3.`object`

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.loyalty.testing.s3.actor.BucketOperationsBehavior.InitiateMultiPartUploadWrapper
import com.loyalty.testing.s3.actor.SpawnBehavior.Command
import com.loyalty.testing.s3.actor.{InvalidAccess, MultiPartUploadedInitiated, NoSuchBucketExists}
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.response.{InitiateMultipartUploadResult, InternalServiceException, NoSuchBucketException}
import com.loyalty.testing.s3.routes.CustomMarshallers
import com.loyalty.testing.s3.routes.s3._

import scala.util.{Failure, Success}

object InitiateMultipartUploadRoute extends CustomMarshallers {
  def apply(bucketName: String,
            key: String,
            objectIO: ObjectIO,
            database: NitriteDatabase)
           (implicit system: ActorSystem[Command],
            timeout: Timeout): Route =
    parameter("uploads") { _ =>
      import system.executionContext
      val eventualEvent =
        for {
          actorRef <- spawnBucketBehavior(bucketName, objectIO, database)
          event <- askBucketBehavior(actorRef, replyTo => InitiateMultiPartUploadWrapper(key, replyTo))
        } yield event
      onComplete(eventualEvent) {
        case Success(MultiPartUploadedInitiated(uploadId)) =>
          complete(InitiateMultipartUploadResult(bucketName, key, uploadId))
        case Success(NoSuchBucketExists) => complete(NoSuchBucketException(bucketName))
        case Success(InvalidAccess) =>
          system.log.warn("InitiateMultipartUploadRoute: invalid access to actor. bucket_name={}, key={}", bucketName,
            key)
          complete(InternalServiceException(s"$bucketName/$key"))
        case Success(event) =>
          system.log.warn("InitiateMultipartUploadRoute: invalid event received. event={}, bucket_name={}, key={}",
            event, bucketName, key)
          complete(InternalServiceException(s"$bucketName/$key"))
        case Failure(ex: Throwable) =>
          system.log.error(s"InitiateMultipartUploadRoute: Internal service error occurred, bucket_name=$bucketName, " +
            s"key=$key", ex)
          complete(InternalServiceException(s"$bucketName/$key"))
      }
    }
}
