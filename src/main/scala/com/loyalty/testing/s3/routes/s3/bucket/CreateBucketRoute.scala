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
import com.loyalty.testing.s3.actor.model.bucket.{Command, CreateBucket}
import com.loyalty.testing.s3.actor.model.{BucketAlreadyExists, BucketInfo, Event}
import com.loyalty.testing.s3.repositories.model.Bucket
import com.loyalty.testing.s3.request.{BucketVersioning, CreateBucketConfiguration}
import com.loyalty.testing.s3.response.{BucketAlreadyExistsException, InternalServiceException}
import com.loyalty.testing.s3.routes.CustomMarshallers
import com.loyalty.testing.s3.routes.s3._

import scala.util.{Failure, Success}

object CreateBucketRoute extends CustomMarshallers {
  def apply(bucketName: String,
            bucketOperationsActorRef: ActorRef[ShardingEnvelope[Command]])
           (implicit system: ActorSystem[_],
            timeout: Timeout): Route =
    (put & extractRequest) { request =>
      val eventualEvent =
        extractRequestTo(request)
          .map(Option.apply)
          .map(CreateBucketConfiguration.apply)
          .map {
            bucketConfiguration =>
              system.log.info("Got request to create bucket {} with bucket configuration {}", bucketName, bucketConfiguration)
              Bucket(bucketName, bucketConfiguration, BucketVersioning.NotExists)
          }
          .via(
            ActorFlow.ask(bucketOperationsActorRef)(
              (bucket, replyTo: ActorRef[Event]) =>
                ShardingEnvelope(bucket.bucketName.toUUID.toString, CreateBucket(bucket, replyTo))
            )
          ).runWith(Sink.head)

      onComplete(eventualEvent) {
        case Success(BucketInfo(bucket)) =>
          complete(HttpResponse(OK).withHeaders(Location(s"/${bucket.bucketName}")))
        case Success(BucketAlreadyExists(bucket)) => complete(BucketAlreadyExistsException(bucket.bucketName))
        case Success(event: Event) =>
          system.log.warn("CreateBucketRoute: invalid event received. event={}, bucket_name={}", event, bucketName)
          complete(InternalServiceException(bucketName))
        case Failure(ex: Throwable) =>
          system.log.error("CreateBucketRoute: Internal service error occurred", ex)
          complete(InternalServiceException(bucketName))
      }
    }
}
