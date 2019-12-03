package com.loyalty.testing.s3.routes.s3.`object`

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.loyalty.testing.s3.actor.BucketOperationsBehavior.UploadPartWrapper
import com.loyalty.testing.s3.actor.SpawnBehavior.Command
import com.loyalty.testing.s3.actor.{InvalidAccess, NoSuchBucketExists, PartUploaded, UploadNotFound}
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.response.{InternalServiceException, NoSuchBucketException, NoSuchUploadException}
import com.loyalty.testing.s3.routes.CustomMarshallers
import com.loyalty.testing.s3.routes.s3._

import scala.util.{Failure, Success}

object UploadPartRoute extends CustomMarshallers {

  def apply(bucketName: String,
            key: String,
            objectIO: ObjectIO,
            database: NitriteDatabase)
           (implicit system: ActorSystem[Command],
            timeout: Timeout): Route =
    (extractRequest & parameter("partNumber".as[Int]) & parameter("uploadId")) {
      (request, partNumber, uploadId) =>
        import system.executionContext
        val eventualEvent =
          for {
            actorRef <- spawnBucketBehavior(bucketName, objectIO, database)
            event <- askBucketBehavior(actorRef, replyTo => UploadPartWrapper(key, uploadId, partNumber,
              request.entity.dataBytes, replyTo))
          } yield event
        onComplete(eventualEvent) {
          case Success(PartUploaded(uploadInfo)) => complete(HttpResponse(OK)
            .withHeaders(createResponseHeaders(uploadInfo.toObjectKey)))
          case Success(NoSuchBucketExists) => complete(NoSuchBucketException(bucketName))
          case Success(UploadNotFound) => complete(NoSuchUploadException(bucketName, key))
          case Success(InvalidAccess) =>
            system.log.warn("UploadPartRoute: invalid access to actor. bucket_name={}, key={}", bucketName, key)
            complete(InternalServiceException(s"$bucketName/$key"))
          case Success(event) =>
            system.log.warn("UploadPartRoute: invalid event received. event={}, bucket_name={}, key={}", event, bucketName, key)
            complete(InternalServiceException(s"$bucketName/$key"))
          case Failure(ex: Throwable) =>
            system.log.error(s"UploadPartRoute: Internal service error occurred, bucket_name=$bucketName, key=$key", ex)
            complete(InternalServiceException(s"$bucketName/$key"))
        }
    }
}
