package com.loyalty.testing.s3.routes.s3.bucket

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.loyalty.testing.s3.actor.BucketOperationsBehavior._
import com.loyalty.testing.s3.actor.SpawnBehavior.Command
import com.loyalty.testing.s3.actor.{BucketInfo, Event}
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.request.VersioningConfiguration
import com.loyalty.testing.s3.response.InternalServiceException
import com.loyalty.testing.s3.routes.CustomMarshallers
import com.loyalty.testing.s3.routes.s3._

import scala.util.{Failure, Success}

object SetBucketVersioningRoute extends CustomMarshallers {
  def apply(bucketName: String,
            objectIO: ObjectIO,
            database: NitriteDatabase)
           (implicit system: ActorSystem[Command],
            timeout: Timeout): Route =
    (put & entity(as[Option[String]]) & parameter("versioning")) {
      (maybeXml, _) =>
        import system.executionContext
        val vc = VersioningConfiguration(maybeXml)
        val eventualEvent =
          for {
            actorRef <- spawnBucketBehavior(bucketName, objectIO, database)
            event <- askBucketBehavior(actorRef, replyTo => SetBucketVersioning(vc.get, replyTo))
          } yield event
        onComplete(eventualEvent) {
          case Success(BucketInfo(bucket)) =>
            complete(HttpResponse(OK).withHeaders(Location(s"/${bucket.bucketName}")))
          case Success(event: Event) =>
            system.log.warn("SetBucketVersioningRoute: invalid event received. event={}, bucket_name={}", event, bucketName)
            complete(InternalServiceException(bucketName))
          case Failure(ex: Throwable) =>
            system.log.error("SetBucketVersioningRoute: Internal service error occurred", ex)
            complete(InternalServiceException(bucketName))
        }
    }

}
