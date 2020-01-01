package com.loyalty.testing.s3.routes.s3.`object`

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.NotFound
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.typed.scaladsl.ActorFlow
import akka.util.Timeout
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.CopyBehavior.{Command, CopyPart}
import com.loyalty.testing.s3.actor.model._
import com.loyalty.testing.s3.response.{CopyPartResult, InternalServiceResponse, NoSuchBucketResponse, NoSuchKeyResponse}
import com.loyalty.testing.s3.routes.CustomMarshallers
import com.loyalty.testing.s3.routes.s3._
import com.loyalty.testing.s3.routes.s3.`object`.directives.{`x-amz-copy-source-range`, `x-amz-copy-source`}

import scala.util.{Failure, Success}

object CopyPartRoute extends CustomMarshallers {

  def apply(bucketName: String,
            key: String,
            copyActorRef: ActorRef[ShardingEnvelope[Command]])
           (implicit system: ActorSystem[_],
            timeout: Timeout): Route =
    (parameter("partNumber".as[Int]) & parameter("uploadId") &
      headerValueByType[`x-amz-copy-source`](()) & optionalHeaderValueByType[`x-amz-copy-source-range`](())) {
      (partNumber, rawUploadId, copySource, maybeCopySourceRange) =>
        val sourceBucketName = copySource.bucketName
        val sourceKey = copySource.key
        val maybeSourceVersionId = copySource.maybeVersionId
        val sourceBucketId = sourceBucketName.toUUID
        val targetBucketId = bucketName.toUUID
        val maybeRange = maybeCopySourceRange.map(_.range)
        val uploadId = rawUploadId.decode.trim
        val eventualEvent =
          Source
            .single("")
            .via(
              ActorFlow.ask(copyActorRef)(
                (_, replyTo: ActorRef[Event]) =>
                  ShardingEnvelope(sourceBucketId.toString, CopyPart(sourceBucketName, sourceKey, bucketName, key,
                    uploadId, partNumber, maybeRange, maybeSourceVersionId, replyTo))
              )
            ).runWith(Sink.head)
        onComplete(eventualEvent) {
          case Success(ObjectInfo(objectKey)) if objectKey.deleteMarker.contains(true) =>
            complete(HttpResponse(NotFound).withHeaders(createResponseHeaders(objectKey)))
          case Success(CopyPartInfo(uploadInfo, sourceVersionId)) =>
            val objectKey = uploadInfo.toObjectKey
            complete(CopyPartResult(objectKey.eTag, objectKey.actualVersionId, sourceVersionId))
          case Success(ObjectInfo(_)) =>
            system.log.warn("CopyPartRoute: invalid access to actor. bucket_name={}, key={}", bucketName, key)
            complete(InternalServiceResponse(s"$bucketName/$key"))
          case Success(NoSuchBucketExists(bucketId)) if bucketId == sourceBucketId =>
            complete(NoSuchBucketResponse(sourceBucketName))
          case Success(NoSuchBucketExists(bucketId)) if bucketId == targetBucketId =>
            complete(NoSuchBucketResponse(bucketName))
          case Success(NoSuchKeyExists(bucketName, key)) => complete(NoSuchKeyResponse(bucketName, key))
          case Success(InvalidAccess) =>
            system.log.warn("CopyPartRoute: invalid access to actor. bucket_name={}, key={}", bucketName, key)
            complete(InternalServiceResponse(s"$bucketName/$key"))
          case Success(event) =>
            system.log.warn("CopyPartRoute: invalid event received. event={}, bucket_name={}, key={}", event, bucketName, key)
            complete(InternalServiceResponse(s"$bucketName/$key"))
          case Failure(ex: Throwable) =>
            system.log.error(s"CopyPartRoute: Internal service error occurred, bucket_name=$bucketName, key=$key", ex)
            complete(InternalServiceResponse(s"$bucketName/$key"))
        }
    }
}
