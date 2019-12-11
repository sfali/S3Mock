package com.loyalty.testing.s3.routes.s3.bucket

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.loyalty.testing.s3.actor.SpawnBehavior.Command
import com.loyalty.testing.s3.actor.model.{BucketInfo, Event, NoSuchBucketExists}
import com.loyalty.testing.s3.actor.model.bucket.SetBucketVersioning
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.request.VersioningConfiguration
import com.loyalty.testing.s3.response.{InternalServiceException, NoSuchBucketException}
import com.loyalty.testing.s3.routes.CustomMarshallers
import com.loyalty.testing.s3.routes.s3._

import scala.util.{Failure, Success}

object SetBucketVersioningRoute extends CustomMarshallers {
  def apply(bucketName: String,
            objectIO: ObjectIO,
            database: NitriteDatabase)
           (implicit system: ActorSystem[Command],
            timeout: Timeout): Route =
    (put & extractRequest & parameter("versioning")) {
      (request, _) =>
        import system.executionContext

        val eventualEvent =
          extractRequestTo(request)
            .map(VersioningConfiguration.apply)
            .flatMap(execute(bucketName, objectIO, database))
        onComplete(eventualEvent) {
          case Success(BucketInfo(bucket)) =>
            complete(HttpResponse(OK).withHeaders(Location(s"/${bucket.bucketName}")))
          case Success(NoSuchBucketExists(_)) => complete(NoSuchBucketException(bucketName))
          case Success(event: Event) =>
            system.log.warn("SetBucketVersioningRoute: invalid event received. event={}, bucket_name={}", event, bucketName)
            complete(InternalServiceException(bucketName))
          case Failure(ex: Throwable) =>
            system.log.error("SetBucketVersioningRoute: Internal service error occurred", ex)
            complete(InternalServiceException(bucketName))
        }
    }

  private def execute(bucketName: String,
                      objectIO: ObjectIO,
                      database: NitriteDatabase)
                     (maybeVersioningConfiguration: Option[VersioningConfiguration])
                     (implicit system: ActorSystem[Command],
                      timeout: Timeout) = {
    import system.executionContext

    system.log.info("Setting bucket versioning: {} on bucket: {}", maybeVersioningConfiguration, bucketName)
    for {
      actorRef <- spawnBucketBehavior(bucketName, objectIO, database)
      event <- askBucketBehavior(actorRef, replyTo => SetBucketVersioning(maybeVersioningConfiguration.get,
        replyTo))
    } yield event
  }

}
