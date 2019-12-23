package com.loyalty.testing.s3.routes

import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.{HttpHeader, HttpRequest}
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.repositories.model.ObjectKey

import scala.concurrent.Future

package object s3 {
  def createResponseHeaders(objectKey: ObjectKey): List[HttpHeader] =
    Nil +
      (CONTENT_MD5, objectKey.contentMd5) +
      (ETAG, s""""${objectKey.eTag}"""") +
      (VersionIdHeader, objectKey.actualVersionId) +
      (DeleteMarkerHeader, objectKey.deleteMarker.filter(_ == true).map(_.toString))

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
