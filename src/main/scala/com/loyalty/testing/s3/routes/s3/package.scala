package com.loyalty.testing.s3.routes

import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.{HttpHeader, HttpRequest}
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.SpawnBehavior.{Spawn, Command => SpawnCommand}
import com.loyalty.testing.s3.actor.model.bucket.Command
import com.loyalty.testing.s3.actor.BucketOperationsBehavior
import com.loyalty.testing.s3.actor.model.Event
import com.loyalty.testing.s3.repositories.model.ObjectKey
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

package object s3 {

  def spawnBucketBehavior(bucketName: String,
                          objectIO: ObjectIO,
                          database: NitriteDatabase)
                         (implicit system: ActorSystem[SpawnCommand],
                          timeout: Timeout): Future[ActorRef[Command]] =
    system.ask[ActorRef[Command]](Spawn(BucketOperationsBehavior(objectIO, database),
      bucketName.toUUID.toString, _))

  def askBucketBehavior(actorRef: ActorRef[Command],
                        toProtocol: ActorRef[Event] => Command)
                       (implicit system: ActorSystem[SpawnCommand],
                        timeout: Timeout): Future[Event] = actorRef.ask[Event](toProtocol)

  def createResponseHeaders(objectKey: ObjectKey): List[RawHeader] = {
    val headers = ListBuffer[RawHeader]()
    if (objectKey.contentMd5.nonEmpty) headers.addOne(RawHeader(CONTENT_MD5, objectKey.contentMd5))
    if (objectKey.eTag.nonEmpty) headers.addOne(RawHeader(ETAG, s""""${objectKey.eTag}""""))
    val deleteMarker = objectKey.deleteMarker.getOrElse(false)
    if (deleteMarker) headers.addOne(RawHeader(DeleteMarkerHeader, deleteMarker.toString))
    val maybeVersionId = objectKey.actualVersionId
    if (maybeVersionId.isDefined) headers.addOne(RawHeader(VersionIdHeader, maybeVersionId.get))
    headers.toList
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
