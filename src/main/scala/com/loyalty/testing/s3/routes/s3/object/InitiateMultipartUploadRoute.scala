package com.loyalty.testing.s3.routes.s3.`object`

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.typed.scaladsl.ActorFlow
import akka.util.Timeout
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.model.bucket.{Command, InitiateMultiPartUploadWrapper}
import com.loyalty.testing.s3.actor.model.{Event, InvalidAccess, MultiPartUploadedInitiated, NoSuchBucketExists}
import com.loyalty.testing.s3.response.{InitiateMultipartUploadResult, InternalServiceResponse, NoSuchBucketResponse}
import com.loyalty.testing.s3.routes.CustomMarshallers

import scala.util.{Failure, Success}

object InitiateMultipartUploadRoute extends CustomMarshallers {
  def apply(bucketName: String,
            key: String,
            bucketOperationsActorRef: ActorRef[ShardingEnvelope[Command]])
           (implicit system: ActorSystem[_],
            timeout: Timeout): Route =
    parameter("uploads") { _ =>
      val eventualEvent =
        Source
          .single("")
          .via(
            ActorFlow.ask(bucketOperationsActorRef)(
              (_, replyTo: ActorRef[Event]) =>
                ShardingEnvelope(bucketName.toUUID.toString, InitiateMultiPartUploadWrapper(key, replyTo))
            )
          ).runWith(Sink.head)
      onComplete(eventualEvent) {
        case Success(MultiPartUploadedInitiated(uploadId)) =>
          complete(InitiateMultipartUploadResult(bucketName, key, uploadId))
        case Success(NoSuchBucketExists(_)) => complete(NoSuchBucketResponse(bucketName))
        case Success(InvalidAccess) =>
          system.log.warn("InitiateMultipartUploadRoute: invalid access to actor. bucket_name={}, key={}", bucketName,
            key)
          complete(InternalServiceResponse(s"$bucketName/$key"))
        case Success(event) =>
          system.log.warn("InitiateMultipartUploadRoute: invalid event received. event={}, bucket_name={}, key={}",
            event, bucketName, key)
          complete(InternalServiceResponse(s"$bucketName/$key"))
        case Failure(ex: Throwable) =>
          system.log.error(s"InitiateMultipartUploadRoute: Internal service error occurred, bucket_name=$bucketName, " +
            s"key=$key", ex)
          complete(InternalServiceResponse(s"$bucketName/$key"))
      }
    }
}
