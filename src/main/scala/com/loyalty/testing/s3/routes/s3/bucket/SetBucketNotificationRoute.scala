package com.loyalty.testing.s3.routes.s3.bucket

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.Sink
import akka.stream.typed.scaladsl.ActorFlow
import akka.util.Timeout
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.NotificationBehavior.{Command, CreateBucketNotifications}
import com.loyalty.testing.s3.actor.model.{Event, NoSuchBucketExists, NotificationsCreated}
import com.loyalty.testing.s3.response.{InternalServiceException, NoSuchBucketException}
import com.loyalty.testing.s3.routes.CustomMarshallers
import com.loyalty.testing.s3.routes.s3._

import scala.util.{Failure, Success}

object SetBucketNotificationRoute extends CustomMarshallers {
  def apply(bucketName: String,
            notificationActorRef: ActorRef[ShardingEnvelope[Command]])
           (implicit system: ActorSystem[_],
            timeout: Timeout): Route =
    (put & extractRequest & parameter("notification")) {
      (request, _) =>
        val eventualEvent =
          extractRequestTo(request)
            .map(s => parseNotificationConfiguration(bucketName, s))
            .via(
              ActorFlow.ask(notificationActorRef)(
                (notifications, replyTo: ActorRef[Event]) =>
                  ShardingEnvelope(
                    bucketName.toUUID.toString, CreateBucketNotifications(notifications, replyTo)
                  )
              )
            ).runWith(Sink.head)
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
}
