package com.loyalty.testing.s3.routes.s3.`object`

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.typed.scaladsl.ActorFlow
import akka.util.Timeout
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.model.bucket.{Command, PutObjectWrapper}
import com.loyalty.testing.s3.actor.model.{Event, InvalidAccess, NoSuchBucketExists, ObjectInfo}
import com.loyalty.testing.s3.response.{InternalServiceResponse, NoSuchBucketResponse}
import com.loyalty.testing.s3.routes.CustomMarshallers
import com.loyalty.testing.s3.routes.s3._

import scala.util.{Failure, Success}

object PutObjectRoute extends CustomMarshallers {

  def apply(bucketName: String,
            key: String,
            bucketOperationsActorRef: ActorRef[ShardingEnvelope[Command]])
           (implicit system: ActorSystem[_],
            timeout: Timeout): Route =
    extractRequest { request =>
      val eventualEvent =
        Source
          .single("")
          .via(
            ActorFlow.ask(bucketOperationsActorRef)(
              (_, replyTo: ActorRef[Event]) =>
                ShardingEnvelope(bucketName.toUUID.toString, PutObjectWrapper(key, request.entity.dataBytes,
                  copy = false, replyTo))
            )
          ).runWith(Sink.head)
      onComplete(eventualEvent) {
        case Success(ObjectInfo(objectKey)) => complete(HttpResponse(OK).withHeaders(createResponseHeaders(objectKey)))
        case Success(NoSuchBucketExists(_)) => complete(NoSuchBucketResponse(bucketName))
        case Success(InvalidAccess) =>
          system.log.warn("PutObjectRoute: invalid access to actor. bucket_name={}, key={}", bucketName, key)
          complete(InternalServiceResponse(s"$bucketName/$key"))
        case Success(event) =>
          system.log.warn("PutObjectRoute: invalid event received. event={}, bucket_name={}, key={}", event, bucketName, key)
          complete(InternalServiceResponse(s"$bucketName/$key"))
        case Failure(ex: Throwable) =>
          system.log.error(s"PutObjectRoute: Internal service error occurred, bucket_name=$bucketName, key=$key", ex)
          complete(InternalServiceResponse(s"$bucketName/$key"))
      }
    }
}
