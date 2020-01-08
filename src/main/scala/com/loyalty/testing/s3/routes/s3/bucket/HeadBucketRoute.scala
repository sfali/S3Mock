package com.loyalty.testing.s3.routes.s3.bucket

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.{NotFound, OK}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.typed.scaladsl.ActorFlow
import akka.util.Timeout
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.model.bucket.{Command, GetBucket}
import com.loyalty.testing.s3.actor.model.{BucketInfo, Event, NoSuchBucketExists}
import com.loyalty.testing.s3.response.InternalServiceResponse
import com.loyalty.testing.s3.routes.CustomMarshallers

import scala.util.{Failure, Success}

object HeadBucketRoute extends CustomMarshallers {

  def apply(bucketName: String,
            bucketOperationsActorRef: ActorRef[ShardingEnvelope[Command]])
           (implicit system: ActorSystem[_],
            timeout: Timeout): Route =
    head {
      val eventualEvent =
        Source
          .single("")
          .via(
            ActorFlow.ask(bucketOperationsActorRef)(
              (_, replyTo: ActorRef[Event]) =>
                ShardingEnvelope(bucketName.toUUID.toString, GetBucket(replyTo))
            )
          )
          .runWith(Sink.head)
      onComplete(eventualEvent) {
        case Success(BucketInfo(_)) => complete(HttpResponse(OK))
        case Success(NoSuchBucketExists(_)) => complete(HttpResponse(NotFound))
        case Success(event: Event) =>
          system.log.warn("HeadBucketRoute: invalid event received. event={}, bucket_name={}", event, bucketName)
          complete(InternalServiceResponse(bucketName))
        case Failure(ex: Throwable) =>
          system.log.error("HeadBucketRoute: Internal service error occurred", ex)
          complete(InternalServiceResponse(bucketName))
      }
    }
}
