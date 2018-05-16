package com.loyalty.testing.s3.notification.actor

import akka.actor.{Actor, ActorLogging, Props}
import com.amazonaws.services.sns.AmazonSNSAsync
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.loyalty.testing.s3.Settings
import com.loyalty.testing.s3.notification.NotificationData

import scala.concurrent.Future
import scala.concurrent.duration._

class NotificationActor(sqsClient: AmazonSQSAsync, snsClient: AmazonSNSAsync) extends Actor with ActorLogging {

  import context.dispatcher
  import NotificationActor._
  import com.loyalty.testing.s3.notification._

  override def preStart(): Unit = {
    super.preStart()
    context.setReceiveTimeout(Duration.Undefined)
  }

  override def receive: Receive = {
    case SqsNotification(queueUrl, configName, notificationData) =>
      val message = generateSqsMessage(configName, notificationData)
      log.info("Sending notification: {}", message)

      Future
        .successful(
          sqsClient
            .sendMessageAsync(queueUrl, message)
            .get())
        .recover {
          case ex => log.warning(
            """
              |Unable to send message to queue {}, error message:- {}:{}
            """.stripMargin, queueUrl, ex.getClass.getName, ex.getMessage)
        }

    case SnsNotification(topicArn, configName, notificationData) => throw new RuntimeException("unimplemented")

    case msg => log.warning("Unhandled message: {}", msg)
  }

}

object NotificationActor {

  def props()(implicit settings: Settings): Props =
    Props(new NotificationActor(settings.sqs.sqsClient, settings.sns.snsClient))

  case class SqsNotification(queueUrl: String, configName: String, notificationData: NotificationData)

  case class SnsNotification(topicArn: String, configName: String, notificationData: NotificationData)

}
