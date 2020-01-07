package com.loyalty.testing.s3.routes.s3.bucket

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.NoContent
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.typed.scaladsl.ActorFlow
import akka.util.Timeout
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.model.bucket.{Command, DeleteBucket}
import com.loyalty.testing.s3.actor.model.{BucketDeleted, BucketNotEmpty, Event, NoSuchBucketExists}
import com.loyalty.testing.s3.response.{BucketNotEmptyResponse, InternalServiceResponse, NoSuchBucketResponse}
import com.loyalty.testing.s3.routes.CustomMarshallers

import scala.util.{Failure, Success}

object DeleteBucketRoute extends CustomMarshallers {

  def apply(bucketName: String,
            bucketOperationsActorRef: ActorRef[ShardingEnvelope[Command]])
           (implicit system: ActorSystem[_],
            timeout: Timeout): Route =
    delete {
      val eventualEvent =
        Source
          .single("")
          .via(
            ActorFlow.ask(bucketOperationsActorRef)(
              (_, replyTo: ActorRef[Event]) =>
                ShardingEnvelope(bucketName.toUUID.toString, DeleteBucket(replyTo))
            )
          )
          .runWith(Sink.head)
      onComplete(eventualEvent) {
        case Success(BucketDeleted) => complete(HttpResponse(NoContent))
        case Success(NoSuchBucketExists(_)) => complete(NoSuchBucketResponse(bucketName))
        case Success(BucketNotEmpty(_)) => complete(BucketNotEmptyResponse(bucketName))
        case Success(event: Event) =>
          system.log.warn("DeleteBucket: invalid event received. event={}, bucket_name={}", event, bucketName)
          complete(InternalServiceResponse(bucketName))
        case Failure(ex: Throwable) =>
          system.log.error("DeleteBucket: Internal service error occurred", ex)
          complete(InternalServiceResponse(bucketName))
      }
    }
}
