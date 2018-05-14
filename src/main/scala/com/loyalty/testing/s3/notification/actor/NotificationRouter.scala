package com.loyalty.testing.s3.notification.actor

import akka.actor.{Actor, ActorLogging, Props, Terminated}
import akka.routing.{ActorRefRoutee, RoundRobinRoutingLogic, Router}
import com.loyalty.testing.s3.Settings
import com.loyalty.testing.s3.notification.DestinationType.DestinationType
import com.loyalty.testing.s3.notification.{DestinationType, Notification, NotificationData}

import scala.concurrent.duration._

class NotificationRouter(notifications: List[Notification])(implicit settings: Settings)
  extends Actor
    with ActorLogging {

  import NotificationActor._
  import NotificationRouter._
  import DestinationType._

  private var router: Router = {
    val routees = Vector.fill(5) {
      val r = context.actorOf(NotificationActor.props())
      context.watch(r)
      ActorRefRoutee(r)
    }
    Router(RoundRobinRoutingLogic(), routees)
  }

  override def preStart(): Unit = {
    super.preStart()
    context.setReceiveTimeout(Duration.Undefined)
  }

  override def receive: Receive = {
    case SendNotification(destinationType, name, notificationData) if destinationType == Sqs =>
      router.route(SqsNotification(name, notificationData), sender())

    case SendNotification(destinationType, name, notificationData) if destinationType == Sns =>
      router.route(SnsNotification(name, notificationData), sender())

    case Terminated(a) =>
      router = router.removeRoutee(a)
      val r = context.actorOf(NotificationActor.props())
      context.watch(r)
      router = router.addRoutee(r)

    case msg => log.warning("Unhandled message: {}", msg)
  }
}

object NotificationRouter {

  def props(notifications: List[Notification] = Nil)
           (implicit settings: Settings): Props = Props(new NotificationRouter(notifications))

  case class SendNotification(destinationType: DestinationType,
                              name: String,
                              notificationData: NotificationData)

}
