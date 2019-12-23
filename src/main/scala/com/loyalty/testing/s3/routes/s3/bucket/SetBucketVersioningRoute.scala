package com.loyalty.testing.s3.routes.s3.bucket

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.Sink
import akka.stream.typed.scaladsl.ActorFlow
import akka.util.Timeout
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.model.bucket.{Command, SetBucketVersioning}
import com.loyalty.testing.s3.actor.model.{BucketInfo, Event, NoSuchBucketExists}
import com.loyalty.testing.s3.request.VersioningConfiguration
import com.loyalty.testing.s3.response.{InternalServiceResponse, NoSuchBucketResponse}
import com.loyalty.testing.s3.routes.CustomMarshallers
import com.loyalty.testing.s3.routes.s3._

import scala.util.{Failure, Success}

object SetBucketVersioningRoute extends CustomMarshallers {
  def apply(bucketName: String,
            bucketOperationsActorRef: ActorRef[ShardingEnvelope[Command]])
           (implicit system: ActorSystem[_],
            timeout: Timeout): Route =
    (put & extractRequest & parameter("versioning")) {
      (request, _) =>
        val eventualEvent =
          extractRequestTo(request)
            .map(Option.apply)
            .map(VersioningConfiguration.apply)
            .via(
              ActorFlow.ask(bucketOperationsActorRef)(
                (maybeVersioningConfiguration, replyTo: ActorRef[Event]) =>
                  ShardingEnvelope(bucketName.toUUID.toString, SetBucketVersioning(maybeVersioningConfiguration.get,
                    replyTo))
              )
            ).runWith(Sink.head)
        onComplete(eventualEvent) {
          case Success(BucketInfo(bucket)) =>
            complete(HttpResponse(OK).withHeaders(Location(s"/${bucket.bucketName}")))
          case Success(NoSuchBucketExists(_)) => complete(NoSuchBucketResponse(bucketName))
          case Success(event: Event) =>
            system.log.warn("SetBucketVersioningRoute: invalid event received. event={}, bucket_name={}", event, bucketName)
            complete(InternalServiceResponse(bucketName))
          case Failure(ex: Throwable) =>
            system.log.error("SetBucketVersioningRoute: Internal service error occurred", ex)
            complete(InternalServiceResponse(bucketName))
        }
    }
}
