package com.loyalty.testing.s3.routes.s3.bucket

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.Sink
import akka.stream.typed.scaladsl.ActorFlow
import akka.util.Timeout
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.model.bucket.{Command, DeleteObjects}
import com.loyalty.testing.s3.actor.model.{DeleteObjectsResult, Event, NoSuchBucketExists}
import com.loyalty.testing.s3.request.Delete
import com.loyalty.testing.s3.response.{InternalServiceResponse, NoSuchBucketResponse}
import com.loyalty.testing.s3.routes.CustomMarshallers
import com.loyalty.testing.s3.routes.s3._

import scala.util.{Failure, Success}

object DeleteObjectsRoute extends CustomMarshallers {

  def apply(bucketName: String,
            bucketOperationsActorRef: ActorRef[ShardingEnvelope[Command]])
           (implicit system: ActorSystem[_],
            timeout: Timeout): Route =
    (post & extractRequest & parameter("delete")) { (request, _) =>
      val eventualEvent =
        extractRequestTo(request)
          .map(Option.apply)
          .map(Delete.apply)
          .via(
            ActorFlow.ask(bucketOperationsActorRef)(
              (deleteRequest, replyTo: ActorRef[Event]) =>
                ShardingEnvelope(bucketName.toUUID.toString, DeleteObjects(deleteRequest.objects,
                  deleteRequest.verbose, replyTo))
            )
          )
          .runWith(Sink.head)
      onComplete(eventualEvent) {
        case Success(DeleteObjectsResult(result)) => complete(result)
        case Success(NoSuchBucketExists(_)) => complete(NoSuchBucketResponse(bucketName))
        case Success(event: Event) =>
          system.log.warn("DeleteObjectsRoute: invalid event received. event={}, bucket_name={}", event, bucketName)
          complete(InternalServiceResponse(bucketName))
        case Failure(ex: Throwable) =>
          system.log.error("DeleteObjectsRoute: Internal service error occurred", ex)
          complete(InternalServiceResponse(bucketName))
      }
    }
}
