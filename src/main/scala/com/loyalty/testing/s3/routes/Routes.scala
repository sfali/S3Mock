package com.loyalty.testing.s3.routes

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.CopyBehavior.{Command => CopyCommand}
import com.loyalty.testing.s3.actor.NotificationBehavior.{Command => NotificationCommand}
import com.loyalty.testing.s3.actor.model.bucket.{Command => BucketCommand}
import com.loyalty.testing.s3.routes.s3.`object`._
import com.loyalty.testing.s3.routes.s3.bucket.{CreateBucketRoute, ListBucketRoute, SetBucketNotificationRoute, SetBucketVersioningRoute}

trait Routes {

  protected implicit val timeout: Timeout
  protected implicit val spawnSystem: ActorSystem[_]
  protected val bucketOperationsActorRef: ActorRef[ShardingEnvelope[BucketCommand]]
  protected val copyActorRef: ActorRef[ShardingEnvelope[CopyCommand]]
  protected val notificationActorRef: ActorRef[ShardingEnvelope[NotificationCommand]]

  lazy val routes: Route =
    pathPrefix(Segment) {
      bucketName =>
        val bucketRoutes =
          concat(
            SetBucketVersioningRoute(bucketName, bucketOperationsActorRef),
            SetBucketNotificationRoute(bucketName, notificationActorRef),
            CreateBucketRoute(bucketName, bucketOperationsActorRef),
            ListBucketRoute(bucketName, bucketOperationsActorRef)
          )
        pathSingleSlash {
          bucketRoutes
        } ~ pathEnd {
          bucketRoutes
        } ~ path(RemainingPath) {
          key =>
            val objectName = key.toString().decode.trim
            put {
              CopyPartRoute(bucketName, objectName, copyActorRef) ~
                UploadPartRoute(bucketName, objectName, bucketOperationsActorRef) ~
                CopyObjectRoute(bucketName, objectName, copyActorRef) ~
                PutObjectRoute(bucketName, objectName, bucketOperationsActorRef)
            } ~ get {
              GetObjectRoute(bucketName, objectName, bucketOperationsActorRef)
            } ~ delete {
              DeleteObjectRoute(bucketName, objectName, bucketOperationsActorRef)
            } ~ post {
              InitiateMultipartUploadRoute(bucketName, objectName, bucketOperationsActorRef) ~
                CompleteUploadRoute(bucketName, objectName, bucketOperationsActorRef)
            } ~ head {
              GetObjectRoute(bucketName, objectName, bucketOperationsActorRef)
            }
        }
    } /* end of bucket segment*/
}
