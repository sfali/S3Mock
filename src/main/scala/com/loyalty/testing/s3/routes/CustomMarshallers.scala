package com.loyalty.testing.s3.routes

import java.nio.charset.StandardCharsets.UTF_8

import akka.http.scaladsl.marshalling.Marshaller.fromToEntityMarshaller
import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller, ToResponseMarshaller}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.{ContentType, ContentTypes, HttpEntity, MediaTypes}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import akka.util.ByteString
import com.loyalty.testing.s3.response._
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport

import scala.xml.NodeSeq

trait CustomMarshallers extends ErrorAccumulatingCirceSupport {

  import ContentTypes._
  import akka.http.scaladsl.marshallers.xml.ScalaXmlSupport._

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

  /*implicit val v: ToEntityMarshaller[CreateBucketConfiguration] =
    nodeSeqMarshaller(MediaTypes.`application/xml`) map {
      me =>
        Marshaller.strict {
          v => HttpEntity(`text/plain(UTF-8)`, "")
        }
    }*/

  implicit val InitiateMultipartUploadResultMarshallers: ToEntityMarshaller[InitiateMultipartUploadResult] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val CopyPartResultMarshallers: ToEntityMarshaller[CopyPartResult] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val CompleteMultipartUploadResultMarshallers: ToEntityMarshaller[CompleteMultipartUploadResult] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val ListBucketResultMarshallers: ToEntityMarshaller[ListBucketResult] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val BucketAlreadyExistsExceptionMarshallers: ToEntityMarshaller[BucketAlreadyExistsException] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val NoSuchBucketExceptionMarshallers: ToEntityMarshaller[NoSuchBucketException] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val NoSuchKeyExceptionMarshallers: ToEntityMarshaller[NoSuchKeyException] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val InvalidNotificationConfigurationExceptionMarshallers: ToEntityMarshaller[InvalidNotificationConfigurationException] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val InternalServiceExceptionMarshallers: ToEntityMarshaller[InternalServiceException] =
    xmlResponseMarshallers(`application/octet-stream`)

  private def xmlResponseMarshallers(contentType: ContentType): ToEntityMarshaller[XmlResponse] =
    Marshaller.withFixedContentType(contentType) { result =>
      HttpEntity(contentType, ByteString(result.toXml.toString().getBytes(UTF_8)))
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

  implicit val CompleteMultipartUploadResultResponse: ToResponseMarshaller[CompleteMultipartUploadResult] =
    fromToEntityMarshaller[CompleteMultipartUploadResult](OK)

  implicit val ListBucketResultResponse: ToResponseMarshaller[ListBucketResult] =
    fromToEntityMarshaller[ListBucketResult](OK)

  implicit val InvalidNotificationConfigurationExceptionResponse: ToResponseMarshaller[InvalidNotificationConfigurationException] =
    fromToEntityMarshaller[InvalidNotificationConfigurationException](BadRequest)

  implicit val InternalServiceExceptionResponse: ToResponseMarshaller[InternalServiceException] =
    fromToEntityMarshaller[InternalServiceException](InternalServerError)

  /*implicit val CompleteMultipartUploadResultResponse: ToResponseMarshaller[CompleteMultipartUploadResult] =
    fromStatusCodeAndHeadersAndValue[CompleteMultipartUploadResult]
      .compose {
        result =>
          val headers: List[HttpHeader] =
            result.versionId.fold(List[HttpHeader]()) {
              vId => RawHeader("x-amz-version-id", vId) :: Nil
            }
          (OK, headers, result)
      }*/


  /*implicit def noSuchBucketExceptionResponse(
      implicit entity: ToEntityMarshaller[NoSuchBucketException])
    : ToResponseMarshaller[NoSuchBucketException] =
    fromToEntityMarshaller[NoSuchBucketException](NotFound)*/

}
