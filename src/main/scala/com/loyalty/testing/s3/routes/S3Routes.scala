package com.loyalty.testing.s3.routes

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import com.loyalty.testing.s3.repositories.Repository
import com.loyalty.testing.s3.routes.s3.`object`._
import com.loyalty.testing.s3.routes.s3.bucket._

trait S3Routes {

  import com.loyalty.testing.s3._

  protected implicit val materializer: Materializer
  protected implicit val log: LoggingAdapter
  protected implicit val repository: Repository
  protected val notificationRouter: ActorRef

  lazy val s3Routes: Route =
    pathPrefix(Segment) {
      bucketName =>
        pathSingleSlash {
          concat(
            SetBucketVersioningRoute().route(bucketName),
            SetBucketNotificationRoute().route(bucketName),
            CreateBucketRoute().route(bucketName),
            ListBucketRoute().route(bucketName)
          )
        } ~ pathEnd {
          concat(
            SetBucketVersioningRoute().route(bucketName),
            SetBucketNotificationRoute().route(bucketName),
            CreateBucketRoute().route(bucketName),
            ListBucketRoute().route(bucketName)
          )
        } ~ path(RemainingPath) {
          key =>
            val objectName = key.toString().decode
            concat(
              GetObjectMetadataRoute().route(bucketName, objectName),
              CompleteMultipartUploadRoute(notificationRouter).route(bucketName, objectName),
              CopyMultipartRoute().route(bucketName, objectName),
              CopyObjectRoute(notificationRouter, log, repository).route(bucketName, objectName),
              UploadMultipartRoute().route(bucketName, objectName),
              InitiateMultipartUploadRoute().route(bucketName, objectName),
              PutObjectRoute(notificationRouter).route(bucketName, objectName),
              GetObjectRoute().route(bucketName, objectName),
              DeleteObjectRoute().route(bucketName, objectName)
            )
        }
    } /* end of bucket segment*/
}
