package com.loyalty.testing.s3.routes.s3.bucket

import akka.event.LoggingAdapter
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.loyalty.testing.s3.repositories.Repository
import com.loyalty.testing.s3.routes.CustomMarshallers

class PutBucketNotificationRoute private(log: LoggingAdapter, repository: Repository)
  extends CustomMarshallers {

  def route(bucketName: String): Route = {
    (put & entity(as[String]) & parameter("notification ")) {
      (xml, notification) =>
        complete("")
    }
  }
}

object PutBucketNotificationRoute {
  def apply()(implicit log: LoggingAdapter, repository: Repository): PutBucketNotificationRoute =
    new PutBucketNotificationRoute(log, repository)
}
