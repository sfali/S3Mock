package com.loyalty.testing.s3.notification.actor

import akka.actor.{Actor, ActorLogging, Props}
import com.loyalty.testing.s3.notification.NotificationData
import com.loyalty.testing.s3.{Settings, SnsSettings, SqsSettings}

import scala.concurrent.Future
import scala.concurrent.duration._

class NotificationActor(sqsSettings: SqsSettings, snsSettings: SnsSettings) extends Actor with ActorLogging {

  import NotificationActor._
  import com.loyalty.testing.s3.notification._

  override def preStart(): Unit = {
    super.preStart()
    context.setReceiveTimeout(Duration.Undefined)
  }

  override def receive: Receive = {
    case SqsNotification(name, notificationData) =>
      sqsSettings.maybeSqsClient match {
        case Some(amazonSQSAsync) =>
          Future.successful(amazonSQSAsync.sendMessageAsync(
            sqsSettings.maybeQueueUrl.get,
            generateSqsMessage(name, notificationData)).get())

        case None => log.info("No Sqs client is setup, notification is disabled.")
      }

    case SnsNotification(name, notificationData) => throw new RuntimeException("unimplemented")

    case msg => log.warning("Unhandled message: {}", msg)
  }

}

object NotificationActor {

  def props()(implicit settings: Settings): Props = Props(new NotificationActor(settings.sqs, settings.sns))

  case class SqsNotification(name: String, notificationData: NotificationData)

  case class SnsNotification(name: String, notificationData: NotificationData)
}
