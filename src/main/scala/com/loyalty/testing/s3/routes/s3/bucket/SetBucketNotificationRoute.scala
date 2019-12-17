package com.loyalty.testing.s3.routes.s3.bucket

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.NotificationBehavior.CreateBucketNotifications
import com.loyalty.testing.s3.actor.SpawnBehavior.Command
import com.loyalty.testing.s3.actor.model.{Event, NoSuchBucketExists, NotificationsCreated}
import com.loyalty.testing.s3.notification.Notification
import com.loyalty.testing.s3.repositories.NitriteDatabase
import com.loyalty.testing.s3.response.{InternalServiceException, NoSuchBucketException}
import com.loyalty.testing.s3.routes.CustomMarshallers
import com.loyalty.testing.s3.routes.s3._
import com.loyalty.testing.s3.service.NotificationService

import scala.util.{Failure, Success}

object SetBucketNotificationRoute extends CustomMarshallers {
  def apply(bucketName: String,
            database: NitriteDatabase,
            notificationService: NotificationService)
           (implicit system: ActorSystem[Command],
            timeout: Timeout): Route =
    (put & extractRequest & parameter("notification")) {
      (request, _) =>
        import system.executionContext

        val eventualEvent =
          toBucketNotification(bucketName, request.entity.dataBytes)
            .flatMap(execute(bucketName, database, notificationService))
        onComplete(eventualEvent) {
          case Success(NotificationsCreated) => complete(HttpResponse(OK))
          case Success(NoSuchBucketExists(_)) => complete(NoSuchBucketException(bucketName))
          case Success(event: Event) =>
            system.log.warn("SetBucketNotificationRoute: invalid event received. event={}, bucket_name={}", event, bucketName)
            complete(InternalServiceException(bucketName))
          case Failure(ex: Throwable) =>
            system.log.error("SetBucketNotificationRoute: Internal service error occurred", ex)
            complete(InternalServiceException(bucketName))
        }
    }

  private def execute(bucketName: String,
                      database: NitriteDatabase,
                      notificationService: NotificationService)
                     (notifications: List[Notification])
                     (implicit system: ActorSystem[Command],
                      timeout: Timeout) = {
    import system.executionContext
    for {
      actorRef <- spawnNotificationBehavior(bucketName, database, notificationService)
      event <- askNotificationBehavior(actorRef, replyTo => CreateBucketNotifications(notifications, replyTo))
    } yield event
  }
}
