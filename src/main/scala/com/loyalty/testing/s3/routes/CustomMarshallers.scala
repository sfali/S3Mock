package com.loyalty.testing.s3.routes

import java.time.Instant

import akka.http.scaladsl.marshallers.xml.ScalaXmlSupport
import akka.http.scaladsl.marshalling.Marshaller.{fromStatusCodeAndHeadersAndValue, fromToEntityMarshaller}
import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller, ToResponseMarshaller}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.response._
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport

import scala.xml.NodeSeq

trait CustomMarshallers
  extends ErrorAccumulatingCirceSupport
    with ScalaXmlSupport {

  import ContentTypes._

  implicit val BucketAlreadyExistsExceptionUnmarshaller: FromEntityUnmarshaller[BucketAlreadyExistsException] =
    nodeSeqUnmarshaller(MediaTypes.`application/xml`, `application/octet-stream`) map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x => BucketAlreadyExistsException((x \ "Resource").text)
    }

  implicit val NoSuchBucketExceptionUnmarshaller: FromEntityUnmarshaller[NoSuchBucketException] =
    nodeSeqUnmarshaller(MediaTypes.`application/xml`, `application/octet-stream`) map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x => NoSuchBucketException((x \ "Resource").text)
    }

  implicit val NoSuchKeyExceptionUnmarshaller: FromEntityUnmarshaller[NoSuchKeyException] =
    nodeSeqUnmarshaller(MediaTypes.`application/xml`, `application/octet-stream`) map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x =>
        val resource = (x \ "Resource").text.drop(1) // drop starting '/'
        val indexOfSeparator = resource.indexOf('/')
        val bucketName = resource.substring(0, indexOfSeparator)
        val key = resource.substring(indexOfSeparator + 1)
        NoSuchKeyException(bucketName, key)
    }

  implicit val CopyObjectResultUnmarshaller: FromEntityUnmarshaller[CopyObjectResult] =
    nodeSeqUnmarshaller(MediaTypes.`application/xml`, `application/octet-stream`) map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x =>
        val etag = (x \ "ETag").text.drop(1).dropRight(1)
        val lastModified = Instant.parse((x \ "LastModified").text)
        CopyObjectResult(etag, lastModifiedDate = lastModified)
    }

  /*
    implicit def v(implicit system: ActorSystem[_]): Marshaller[NodeSeq, Future[CreateBucketConfiguration]] =
      nodeSeqMarshaller(MediaTypes.`application/xml`) map {
        case NodeSeq.Empty => Future.successful(CreateBucketConfiguration())
        case x =>
          import system.executionContext
          x.dataBytes.map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).map {
            s =>
              CreateBucketConfiguration()
          }
      }*/

  implicit val InitiateMultipartUploadResultMarshallers: ToEntityMarshaller[InitiateMultipartUploadResult] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val CopyObjectResultMarshallers: ToEntityMarshaller[CopyObjectResult] =
    xmlResponseMarshallers(`text/xml(UTF-8)`)

  implicit val CopyPartResultMarshallers: ToEntityMarshaller[CopyPartResult] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val CompleteMultipartUploadResultMarshallers: ToEntityMarshaller[CompleteMultipartUploadResult] =
    xmlResponseMarshallers(`text/xml(UTF-8)`)

  implicit val ListBucketResultMarshallers: ToEntityMarshaller[ListBucketResult] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val BucketAlreadyExistsExceptionMarshallers: ToEntityMarshaller[BucketAlreadyExistsException] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val NoSuchBucketExceptionMarshallers: ToEntityMarshaller[NoSuchBucketException] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val NoSuchKeyExceptionMarshallers: ToEntityMarshaller[NoSuchKeyException] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val NoSuchUploadExceptionMarshallers: ToEntityMarshaller[NoSuchUploadException] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val InvalidPartExceptionMarshallers: ToEntityMarshaller[InvalidPartException] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val InvalidPartOrderExceptionMarshallers: ToEntityMarshaller[InvalidPartOrderException] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val InvalidNotificationConfigurationExceptionMarshallers: ToEntityMarshaller[InvalidNotificationConfigurationException] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val InternalServiceExceptionMarshallers: ToEntityMarshaller[InternalServiceException] =
    xmlResponseMarshallers(`application/octet-stream`)

  private def xmlResponseMarshallers(contentType: ContentType): ToEntityMarshaller[XmlResponse] =
    Marshaller.withFixedContentType(contentType) { result =>
      contentType match {
        case `text/xml(UTF-8)` => HttpEntity(MediaTypes.`application/xml`.toContentType(HttpCharsets.`UTF-8`),
          result.toByteString.utf8String)
        case `application/octet-stream` => HttpEntity(contentType, result.toByteString)
        case _ => throw new RuntimeException(s"unsupported content type: $contentType")
      }
    }

  implicit val InitiateMultipartUploadResultResponse: ToResponseMarshaller[InitiateMultipartUploadResult] =
    fromToEntityMarshaller[InitiateMultipartUploadResult](OK)

  implicit val CopyPartResultResponse: ToResponseMarshaller[CopyPartResult] =
    fromToEntityMarshaller[CopyPartResult](OK)

  implicit val BucketAlreadyExistsExceptionResponse: ToResponseMarshaller[BucketAlreadyExistsException] =
    fromToEntityMarshaller[BucketAlreadyExistsException](BadRequest)

  implicit val NoSuchBucketExceptionResponse: ToResponseMarshaller[NoSuchBucketException] =
    fromToEntityMarshaller[NoSuchBucketException](NotFound)

  implicit val NoSuchKeyExceptionResponse: ToResponseMarshaller[NoSuchKeyException] =
    fromToEntityMarshaller[NoSuchKeyException](NotFound)

  implicit val NoSuchUploadExceptionResponse: ToResponseMarshaller[NoSuchUploadException] =
    fromToEntityMarshaller[NoSuchUploadException](NotFound)

  implicit val InvalidPartOrderExceptionResponse: ToResponseMarshaller[InvalidPartOrderException] =
    fromToEntityMarshaller[InvalidPartOrderException](BadRequest)

  implicit val InvalidPartExceptionResponse: ToResponseMarshaller[InvalidPartException] =
    fromToEntityMarshaller[InvalidPartException](BadRequest)

  implicit val ListBucketResultResponse: ToResponseMarshaller[ListBucketResult] =
    fromToEntityMarshaller[ListBucketResult](OK)

  implicit val InvalidNotificationConfigurationExceptionResponse: ToResponseMarshaller[InvalidNotificationConfigurationException] =
    fromToEntityMarshaller[InvalidNotificationConfigurationException](BadRequest)

  implicit val InternalServiceExceptionResponse: ToResponseMarshaller[InternalServiceException] =
    fromToEntityMarshaller[InternalServiceException](InternalServerError)

  implicit val CompleteMultipartUploadResultResponse: ToResponseMarshaller[CompleteMultipartUploadResult] =
    fromStatusCodeAndHeadersAndValue[CompleteMultipartUploadResult]
      .compose { result =>
        val headers = Nil + (VersionIdHeader, result.versionId)
        (OK, headers, result)
      }

  implicit val CopyObjectResultResponse: ToResponseMarshaller[CopyObjectResult] =
    fromStatusCodeAndHeadersAndValue[CopyObjectResult]
      .compose { result =>
        val headers = Nil +
          ("x-amz-copy-source-version-id", result.maybeSourceVersionId) +
          (VersionIdHeader, result.maybeVersionId)
        (OK, headers, result)
      }


  /*implicit def noSuchBucketExceptionResponse(
      implicit entity: ToEntityMarshaller[NoSuchBucketException])
    : ToResponseMarshaller[NoSuchBucketException] =
    fromToEntityMarshaller[NoSuchBucketException](NotFound)*/

}
