package com.loyalty.testing.s3.routes.s3.`object`

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.Sink
import akka.stream.typed.scaladsl.ActorFlow
import akka.util.Timeout
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.model._
import com.loyalty.testing.s3.actor.model.bucket.{Command, CompleteUploadWrapper}
import com.loyalty.testing.s3.request.CompleteMultipartUpload
import com.loyalty.testing.s3.response._
import com.loyalty.testing.s3.routes.CustomMarshallers
import com.loyalty.testing.s3.routes.s3._

import scala.util.{Failure, Success}

object CompleteUploadRoute extends CustomMarshallers {

  def apply(bucketName: String,
            key: String,
            bucketOperationsActorRef: ActorRef[ShardingEnvelope[Command]])
           (implicit system: ActorSystem[_],
            timeout: Timeout): Route =
    (extractRequest & parameter("uploadId")) { (request, rawUploadId) =>
      val uploadId = rawUploadId.decode.trim
      val eventualEvent =
        extractRequestTo(request)
          .map(Option.apply)
          .map(CompleteMultipartUpload.apply)
          .via(
            ActorFlow.ask(bucketOperationsActorRef)(
              (maybeCompleteMultipartUpload, replyTo: ActorRef[Event]) => {
                val parts = maybeCompleteMultipartUpload.map(_.parts).getOrElse(Nil)
                ShardingEnvelope(bucketName.toUUID.toString, CompleteUploadWrapper(key, uploadId, parts, replyTo))
              }
            )
          ).runWith(Sink.head)
      onComplete(eventualEvent) {
        case Success(ObjectInfo(objectKey)) =>
          val result = CompleteMultipartUploadResult(bucketName, key, objectKey.eTag.getOrElse(""), objectKey.contentLength,
            objectKey.actualVersionId)
          complete(result)
        case Success(NoSuchBucketExists(_)) => complete(NoSuchBucketResponse(bucketName))
        case Success(NoSuchUpload) => complete(NoSuchUploadResponse(bucketName, key))
        case Success(InvalidPartOrder) => complete(InvalidPartOrderResponse(bucketName, key))
        case Success(InvalidPart(partNumber)) => complete(InvalidPartResponse(bucketName, key, partNumber, uploadId))
        case Success(InvalidAccess) =>
          system.log.warn("CompleteUploadRoute: invalid access to actor. bucket_name={}, key={}", bucketName, key)
          complete(InternalServiceResponse(s"$bucketName/$key"))
        case Success(event) =>
          system.log.warn("CompleteUploadRoute: invalid event received. event={}, bucket_name={}, key={}", event, bucketName, key)
          complete(InternalServiceResponse(s"$bucketName/$key"))
        case Failure(ex: Throwable) =>
          system.log.error(s"CompleteUploadRoute: Internal service error occurred, bucket_name=$bucketName, key=$key", ex)
          complete(InternalServiceResponse(s"$bucketName/$key"))
      }
    }
}
