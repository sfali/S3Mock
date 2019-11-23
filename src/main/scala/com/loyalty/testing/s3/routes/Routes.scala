package com.loyalty.testing.s3.routes

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.SpawnBehavior.Command
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.routes.s3.`object`.PutObjectRoute
import com.loyalty.testing.s3.routes.s3.bucket.CreateBucketRoute

trait Routes {

  protected implicit val timeout: Timeout
  protected implicit val spawnSystem: ActorSystem[Command]
  protected val objectIO: ObjectIO
  protected val database: NitriteDatabase

  lazy val routes: Route =
    pathPrefix(Segment) {
      bucketName =>
        val bucketRoutes = concat(
          CreateBucketRoute(bucketName, objectIO, database)
        )
        pathSingleSlash {
          bucketRoutes
        } ~ pathEnd {
          bucketRoutes
        } ~ path(RemainingPath) {
          key =>
            val objectName = key.toString().decode
            concat(
              PutObjectRoute(bucketName, objectName, objectIO, database)
            )
        }
    } /* end of bucket segment*/
}
