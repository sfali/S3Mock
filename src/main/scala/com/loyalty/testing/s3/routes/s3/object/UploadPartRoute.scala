package com.loyalty.testing.s3.routes.s3.`object`

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.typed.scaladsl.ActorFlow
import akka.util.Timeout
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.model._
import com.loyalty.testing.s3.actor.model.bucket.{Command, UploadPartWrapper}
import com.loyalty.testing.s3.response.{InternalServiceResponse, NoSuchBucketResponse, NoSuchUploadResponse}
import com.loyalty.testing.s3.routes.CustomMarshallers
import com.loyalty.testing.s3.routes.s3._

import scala.util.{Failure, Success}

object UploadPartRoute extends CustomMarshallers {

  def apply(bucketName: String,
            key: String,
            bucketOperationsActorRef: ActorRef[ShardingEnvelope[Command]])
           (implicit system: ActorSystem[_],
            timeout: Timeout): Route =
    (extractRequest & parameter("partNumber".as[Int]) & parameter("uploadId")) {
      (request, partNumber, rawUploadId) =>
        val uploadId = rawUploadId.decode.trim
        system.log.info("Upload part: bucket_name={}, key={}, upload_id={}, part_number={}", bucketName, key, uploadId,
          partNumber)
        val eventualEvent =
          Source
            .single("")
            .via(
              ActorFlow.ask(bucketOperationsActorRef)(
                (_, replyTo: ActorRef[Event]) =>
                  ShardingEnvelope(bucketName.toUUID.toString, UploadPartWrapper(key, uploadId, partNumber,
                    request.entity.dataBytes, replyTo))
              )
            ).runWith(Sink.head)
        onComplete(eventualEvent) {
          case Success(PartUploaded(uploadInfo)) => complete(HttpResponse(OK)
            .withHeaders(createResponseHeaders(uploadInfo.toObjectKey)))
          case Success(NoSuchBucketExists(_)) => complete(NoSuchBucketResponse(bucketName))
          case Success(NoSuchUpload) => complete(NoSuchUploadResponse(bucketName, key))
          case Success(InvalidAccess) =>
            system.log.warn("UploadPartRoute: invalid access to actor. bucket_name={}, key={}", bucketName, key)
            complete(InternalServiceResponse(s"$bucketName/$key"))
          case Success(event) =>
            system.log.warn("UploadPartRoute: invalid event received. event={}, bucket_name={}, key={}", event, bucketName, key)
            complete(InternalServiceResponse(s"$bucketName/$key"))
          case Failure(ex: Throwable) =>
            system.log.error(s"UploadPartRoute: Internal service error occurred, bucket_name=$bucketName, key=$key", ex)
            complete(InternalServiceResponse(s"$bucketName/$key"))
        }
    }
}
