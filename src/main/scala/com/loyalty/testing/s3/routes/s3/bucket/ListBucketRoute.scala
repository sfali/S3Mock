package com.loyalty.testing.s3.routes.s3.bucket

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.BadRequest
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.typed.scaladsl.ActorFlow
import akka.util.Timeout
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.model.bucket.{Command, ListBucket}
import com.loyalty.testing.s3.actor.model.{Event, ListBucketContent, NoSuchBucketExists}
import com.loyalty.testing.s3.request.ListBucketParams
import com.loyalty.testing.s3.response.{InternalServiceException, ListBucketResult, NoSuchBucketException}
import com.loyalty.testing.s3.routes.CustomMarshallers

import scala.util.{Failure, Success}

object ListBucketRoute extends CustomMarshallers {

  def apply(bucketName: String,
            bucketOperationsActorRef: ActorRef[ShardingEnvelope[Command]])
           (implicit system: ActorSystem[_],
            timeout: Timeout): Route =
    (get & parameter("list-type".as[Int]) & parameter("max-keys".as[Int].?) & parameter("delimiter".?)
      & parameter("prefix".?)) {
      (listType, maxKeysParam, delimiterParam, prefixParam) =>
        val maybePrefix = prefixParam.filterNot(_ == "")
        val maybeDelimiter = delimiterParam.filterNot(_ == "")
        if (listType != 2) complete(HttpResponse(BadRequest))
        else {
          val maxKeys = maxKeysParam.getOrElse(1000)
          val params = ListBucketParams(maxKeys, maybePrefix, maybeDelimiter)
          val eventualEvent =
            Source
              .single(params)
              .via(
                ActorFlow.ask(bucketOperationsActorRef)(
                  (params, replyTo: ActorRef[Event]) =>
                    ShardingEnvelope(bucketName.toUUID.toString, ListBucket(params, replyTo))
                )
              ).runWith(Sink.head)
          onComplete(eventualEvent) {
            case Success(ListBucketContent(contents)) =>
              val result = ListBucketResult(
                bucketName = bucketName,
                maybePrefix = params.maybePrefix,
                keyCount = contents.length,
                maxKeys = params.maxKeys,
                contents = contents
              )
              complete(result)
            case Success(NoSuchBucketExists(_)) => complete(NoSuchBucketException(bucketName))
            case Success(event: Event) =>
              system.log.warn("ListBucketRoute: invalid event received. event={}, bucket_name={}", event, bucketName)
              complete(InternalServiceException(bucketName))
            case Failure(ex: Throwable) =>
              system.log.error("ListBucketRoute: Internal service error occurred", ex)
              complete(InternalServiceException(bucketName))
          }
        }
    }
}
