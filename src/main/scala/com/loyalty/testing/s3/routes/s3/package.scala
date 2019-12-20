package com.loyalty.testing.s3.routes

import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.{HttpHeader, HttpRequest}
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.repositories.model.ObjectKey

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

package object s3 {
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

  def extractRequestToOld(request: HttpRequest)
                         (implicit mat: Materializer): Future[Option[String]] = {
    import mat.executionContext
    request
      .entity
      .dataBytes
      .map(_.utf8String)
      .runWith(Sink.head)
      .map(s => if (s.isEmpty) None else Some(s))
  }

  def extractRequestTo(request: HttpRequest): Source[String, Any] =
    request
      .entity
      .dataBytes
      .map(_.utf8String)

}
