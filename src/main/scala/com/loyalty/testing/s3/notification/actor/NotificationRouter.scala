package com.loyalty.testing.s3.notification.actor

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Props, Terminated}
import akka.routing.{ActorRefRoutee, RoundRobinRoutingLogic, Router}
import com.loyalty.testing.s3.Settings
import com.loyalty.testing.s3.notification.{DestinationType, Notification, NotificationData}

import scala.concurrent.duration._

class NotificationRouter(notifications: List[Notification])(implicit settings: Settings)
  extends Actor
    with ActorLogging {

  import DestinationType._
  import NotificationActor._
  import NotificationRouter._

  private val routerState = new RouterState(context)

  override def preStart(): Unit = {
    super.preStart()
    context.setReceiveTimeout(Duration.Undefined)
  }

  override def receive: Receive = {
    case SendNotification(notificationData) =>
      if (notifications.nonEmpty) {
        notifications
          .foreach {
            notification =>
              val bucketMatch = notification.bucketName == notificationData.bucketName
              val prefixMatch = notification.prefix.exists(notificationData.key.startsWith)
              val suffixMatch = notification.suffix.exists(notificationData.key.endsWith)
              if (bucketMatch && prefixMatch && suffixMatch) {
                self ! SendNotificationToDestination(notification.destinationType,
                  notification.destinationName, notification.name, notificationData)
              } else {
                log.warning("No match for notification: {} : {}", notification, notificationData)
              }
          }
      } else {
        log.warning("No notification is setup for bucket: {}", notificationData.bucketName)
      }

    case SendNotificationToDestination(destinationType, destinationName, configName, notificationData)
      if destinationType == Sqs =>
      routerState.router.route(SqsNotification(destinationName, configName, notificationData), sender())

    case SendNotificationToDestination(destinationType, destinationName, configName, notificationData)
      if destinationType == Sns =>
      routerState.router.route(SnsNotification(destinationName, configName, notificationData), sender())

    case Terminated(a) => routerState.removeRoutee(a)

    case msg => log.warning("Unhandled message: {}", msg)
  }
}

object NotificationRouter {

  def props(notifications: List[Notification] = Nil)
           (implicit settings: Settings): Props = Props(new NotificationRouter(notifications))

  case class SendNotification(notificationData: NotificationData)

  private case class SendNotificationToDestination(destinationType: DestinationType,
                                                   destinationName: String,
                                                   configName: String,
                                                   notificationData: NotificationData)

  private class RouterState(context: ActorContext)(implicit settings: Settings) {
    private var _router: Router = {
      val routees = Vector.fill(5) {
        val r = context.actorOf(NotificationActor.props())
        context.watch(r)
        ActorRefRoutee(r)
      }
      Router(RoundRobinRoutingLogic(), routees)
    }

    def router: Router = _router

    def removeRoutee(actorRef: ActorRef): Unit = {
      _router = _router.removeRoutee(actorRef)
      val r = context.actorOf(NotificationActor.props())
      context.watch(r)
      _router = _router.addRoutee(r)
    }
  }

}
