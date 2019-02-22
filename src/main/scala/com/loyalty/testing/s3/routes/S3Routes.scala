package com.loyalty.testing.s3.routes

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.loyalty.testing.s3.repositories.Repository
import com.loyalty.testing.s3.routes.s3.`object`._
import com.loyalty.testing.s3.routes.s3.bucket.{CreateBucketRoute, SetBucketVersioningRoute}

trait S3Routes {

  import com.loyalty.testing.s3._

  protected implicit val mat: ActorMaterializer
  protected implicit val log: LoggingAdapter
  protected implicit val repository: Repository
  protected val notificationRouter: ActorRef

  lazy val s3Routes: Route =
    pathPrefix(Segment) { bucketName =>
      pathSingleSlash {
        concat(
          SetBucketVersioningRoute().route(bucketName),
          CreateBucketRoute().route(bucketName)
        )
      } ~ pathEnd {
        concat(
          SetBucketVersioningRoute().route(bucketName),
          CreateBucketRoute().route(bucketName)
        )
      } ~ path(RemainingPath) { key =>
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
