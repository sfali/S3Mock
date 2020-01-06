package com.loyalty.testing.s3.routes.s3.`object`

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
import com.loyalty.testing.s3.actor.model._
import com.loyalty.testing.s3.actor.model.bucket.{Command, DeleteObjectWrapper}
import com.loyalty.testing.s3.response.{InternalServiceResponse, NoSuchBucketResponse, NoSuchKeyResponse}
import com.loyalty.testing.s3.routes.CustomMarshallers
import com.loyalty.testing.s3.routes.s3._

import scala.util.{Failure, Success}

object DeleteObjectRoute extends CustomMarshallers {

  def apply(bucketName: String,
            key: String,
            bucketOperationsActorRef: ActorRef[ShardingEnvelope[Command]])
           (implicit system: ActorSystem[_],
            timeout: Timeout): Route =
    parameter("versionId".?) {
      maybeVersionId =>
        val eventualEvent =
          Source
            .single("")
            .via(
              ActorFlow.ask(bucketOperationsActorRef)(
                (_, replyTo: ActorRef[Event]) =>
                  ShardingEnvelope(bucketName.toUUID.toString, DeleteObjectWrapper(key, maybeVersionId, replyTo))
              )
            ).runWith(Sink.head)
        onComplete(eventualEvent) {
          case Success(ObjectInfo(objectKey)) => complete(HttpResponse(NoContent)
            .withHeaders(createResponseHeaders(objectKey)))
          case Success(NoSuchBucketExists(_)) => complete(NoSuchBucketResponse(bucketName))
          case Success(NoSuchKeyExists(bucketName, key)) => complete(NoSuchKeyResponse(bucketName, key))
          case Success(InvalidAccess) =>
            system.log.warn("DeleteObjectRoute: invalid access to actor. bucket_name={}, key={}", bucketName, key)
            complete(InternalServiceResponse(s"$bucketName/$key"))
          case Success(event) =>
            system.log.warn("DeleteObjectRoute: invalid event received. event={}, bucket_name={}, key={}", event, bucketName, key)
            complete(InternalServiceResponse(s"$bucketName/$key"))
          case Failure(ex: Throwable) =>
            system.log.error(s"DeleteObjectRoute: Internal service error occurred, bucket_name=$bucketName, key=$key", ex)
            complete(InternalServiceResponse(s"$bucketName/$key"))
        }
    }
}
