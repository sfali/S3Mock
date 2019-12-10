package com.loyalty.testing.s3.routes.s3.bucket

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.loyalty.testing.s3.actor.SpawnBehavior.Command
import com.loyalty.testing.s3.actor.model.{BucketAlreadyExists, BucketInfo, Event}
import com.loyalty.testing.s3.actor.model.bucket.CreateBucket
import com.loyalty.testing.s3.repositories.model.Bucket
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.request.{BucketVersioning, CreateBucketConfiguration}
import com.loyalty.testing.s3.response.{BucketAlreadyExistsException, InternalServiceException}
import com.loyalty.testing.s3.routes.CustomMarshallers
import com.loyalty.testing.s3.routes.s3._

import scala.util.{Failure, Success}

object CreateBucketRoute extends CustomMarshallers {
  def apply(bucketName: String,
            objectIO: ObjectIO,
            database: NitriteDatabase)
           (implicit system: ActorSystem[Command],
            timeout: Timeout): Route =
    (put & extractRequest) { request =>
      import system.executionContext

      val eventualEvent =
        extractRequestTo(request)
          .map(CreateBucketConfiguration.apply)
          .map {
            bucketConfiguration =>
              system.log.info("Got request to create bucket {} with bucket configuration {}", bucketName, bucketConfiguration)
              Bucket(bucketName, bucketConfiguration, BucketVersioning.NotExists)
          }
          .flatMap(execute(objectIO, database))

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

  private def execute(objectIO: ObjectIO,
                      database: NitriteDatabase)
                     (bucket: Bucket)
                     (implicit system: ActorSystem[Command],
                      timeout: Timeout) = {
    import system.executionContext
    for {
      actorRef <- spawnBucketBehavior(bucket.bucketName, objectIO, database)
      event <- askBucketBehavior(actorRef, replyTo => CreateBucket(bucket, replyTo))
    } yield event
  }

}
