package com.loyalty.testing.s3.routes.s3.`object`

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.loyalty.testing.s3.{CONTENT_MD5, ETAG}
import com.loyalty.testing.s3.actor.BucketOperationsBehavior.PutObjectWrapper
import com.loyalty.testing.s3.actor.SpawnBehavior.Command
import com.loyalty.testing.s3.actor.{InvalidAccess, NoSuchBucketExists, NoSuchKeyExists, ObjectInfo}
import com.loyalty.testing.s3.repositories.model.ObjectKey
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.request.BucketVersioning
import com.loyalty.testing.s3.response.{InternalServiceException, NoSuchBucketException, NoSuchKeyException}
import com.loyalty.testing.s3.routes.CustomMarshallers
import com.loyalty.testing.s3.routes.s3.{askBucketBehavior, spawnBucketBehavior}

import scala.util.{Failure, Success}

object PutObjectRoute extends CustomMarshallers {

  def apply(bucketName: String,
            key: String,
            objectIO: ObjectIO,
            database: NitriteDatabase)
           (implicit system: ActorSystem[Command],
            timeout: Timeout): Route =
    (put & extractRequest) { request =>
      import system.executionContext

      val eventualEvent =
        for {
          actorRef <- spawnBucketBehavior(bucketName, objectIO, database)
          event <- askBucketBehavior(actorRef, replyTo => PutObjectWrapper(key, request.entity.dataBytes, replyTo))
        } yield event

      onComplete(eventualEvent) {
        case Success(ObjectInfo(objectKey)) => complete(toSuccessResponse(objectKey))
        case Success(NoSuchBucketExists) => complete(NoSuchBucketException(bucketName))
        case Success(NoSuchKeyExists) => complete(NoSuchKeyException(bucketName, key))
        case Success(InvalidAccess) =>
          system.log.warn("PutObjectRoute: invalid access to actor. bucket_name={}, key={}", bucketName, key)
          complete(InternalServiceException(s"$bucketName/$key"))
        case Success(event) =>
          system.log.warn("PutObjectRoute: invalid event received. event={}, bucket_name={}, key={}", event, bucketName, key)
          complete(InternalServiceException(s"$bucketName/$key"))
        case Failure(ex: Throwable) =>
          system.log.error(s"PutObjectRoute: Internal service error occurred, bucket_name=$bucketName, key=$key", ex)
          complete(InternalServiceException(s"$bucketName/$key"))
      }
    }

  private def toSuccessResponse(objectKey: ObjectKey) = {
    var headers = RawHeader(CONTENT_MD5, objectKey.contentMd5) :: RawHeader(ETAG, s""""${objectKey.eTag}"""") :: Nil
    headers =
      objectKey.version match {
        case BucketVersioning.Enabled =>
          headers :+ RawHeader("x-amz-version-id", objectKey.versionId)
        case _ => headers
      }
    HttpResponse(OK).withHeaders(headers)
  }
}
