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
import com.loyalty.testing.s3.actor.CopyBehavior.{Command, Copy}
import com.loyalty.testing.s3.actor.model._
import com.loyalty.testing.s3.response.{CopyObjectResult, InternalServiceResponse, NoSuchBucketResponse, NoSuchKeyResponse}
import com.loyalty.testing.s3.routes.CustomMarshallers
import com.loyalty.testing.s3.routes.s3._

import scala.util.{Failure, Success}

object CopyObjectRoute extends CustomMarshallers {

  import directives._

  def apply(bucketName: String,
            key: String,
            copyActorRef: ActorRef[ShardingEnvelope[Command]])
           (implicit system: ActorSystem[_],
            timeout: Timeout): Route =
    headerValueByType[`x-amz-copy-source`](()) { source =>
      val sourceBucketName = source.bucketName
      val sourceKey = source.key
      val maybeSourceVersionId = source.maybeVersionId
      val sourceBucketId = sourceBucketName.toUUID
      val targetBucketId = bucketName.toUUID
      val eventualEvent =
        Source
          .single("")
          .via(
            ActorFlow.ask(copyActorRef)(
              (_, replyTo: ActorRef[Event]) =>
                ShardingEnvelope(sourceBucketId.toString, Copy(sourceBucketName, sourceKey, bucketName, key,
                  maybeSourceVersionId, replyTo))
            )
          ).runWith(Sink.head)
      onComplete(eventualEvent) {
        case Success(ObjectInfo(objectKey)) if objectKey.deleteMarker.contains(true) =>
          complete(HttpResponse(NotFound).withHeaders(createResponseHeaders(objectKey)))
        case Success(CopyObjectInfo(objectKey, sourceVersionId)) =>
          complete(CopyObjectResult(objectKey.eTag.getOrElse(""), objectKey.actualVersionId, sourceVersionId))
        case Success(ObjectInfo(_)) =>
          system.log.warn("CopyObjectRoute: invalid access to actor. bucket_name={}, key={}", bucketName, key)
          complete(InternalServiceResponse(s"$bucketName/$key"))
        case Success(NoSuchBucketExists(bucketId)) if bucketId == sourceBucketId =>
          complete(NoSuchBucketResponse(sourceBucketName))
        case Success(NoSuchBucketExists(bucketId)) if bucketId == targetBucketId =>
          complete(NoSuchBucketResponse(bucketName))
        case Success(NoSuchKeyExists(bucketName, key)) => complete(NoSuchKeyResponse(bucketName, key))
        case Success(InvalidAccess) =>
          system.log.warn("CopyObjectRoute: invalid access to actor. bucket_name={}, key={}", bucketName, key)
          complete(InternalServiceResponse(s"$bucketName/$key"))
        case Success(event) =>
          system.log.warn("CopyObjectRoute: invalid event received. event={}, bucket_name={}, key={}", event, bucketName, key)
          complete(InternalServiceResponse(s"$bucketName/$key"))
        case Failure(ex: Throwable) =>
          system.log.error(s"CopyObjectRoute: Internal service error occurred, bucket_name=$bucketName, key=$key", ex)
          complete(InternalServiceResponse(s"$bucketName/$key"))
      }
    }
}
