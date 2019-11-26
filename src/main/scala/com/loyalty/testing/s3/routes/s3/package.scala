package com.loyalty.testing.s3.routes

import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.{HttpHeader, HttpRequest}
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.BucketOperationsBehavior.BucketProtocol
import com.loyalty.testing.s3.actor.SpawnBehavior.{Command, Spawn}
import com.loyalty.testing.s3.actor.{BucketOperationsBehavior, Event}
import com.loyalty.testing.s3.repositories.model.ObjectKey
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.request.BucketVersioning

import scala.concurrent.Future

package object s3 {

  def spawnBucketBehavior(bucketName: String,
                          objectIO: ObjectIO,
                          database: NitriteDatabase)
                         (implicit system: ActorSystem[Command],
                          timeout: Timeout): Future[ActorRef[BucketProtocol]] =
    system.ask[ActorRef[BucketProtocol]](Spawn(BucketOperationsBehavior(objectIO, database),
      bucketName.toUUID.toString, _))

  def askBucketBehavior(actorRef: ActorRef[BucketProtocol],
                        toProtocol: ActorRef[Event] => BucketProtocol)
                       (implicit system: ActorSystem[Command],
                        timeout: Timeout): Future[Event] = actorRef.ask[Event](toProtocol)

  def createResponseHeaders(objectKey: ObjectKey): List[RawHeader] = {
    val headers = RawHeader(CONTENT_MD5, objectKey.contentMd5) :: RawHeader(ETAG, s""""${objectKey.eTag}"""") :: Nil
    objectKey.version match {
      case BucketVersioning.Enabled =>
        headers :+ RawHeader("x-amz-version-id", objectKey.versionId)
      case _ => headers
    }
  }

  def extractRange: HttpHeader => Option[ByteRange] = {
    case h: Range => h.ranges.headOption
    case _ => None
  }

  def extractRequestTo(request: HttpRequest)
                      (implicit mat: Materializer): Future[Option[String]] = {
    import mat.executionContext
    request
      .entity
      .dataBytes
      .map(_.utf8String)
      .runWith(Sink.head)
      .map(s => if (s.isEmpty) None else Some(s))
  }

}
