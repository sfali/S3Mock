package com.loyalty.testing.s3.routes

import java.nio.charset.StandardCharsets

import akka.http.scaladsl.marshalling.Marshaller.fromToEntityMarshaller
import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller, ToResponseMarshaller}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.{ContentType, ContentTypes, HttpEntity}
import akka.util.ByteString
import com.loyalty.testing.s3.response._
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport

import scala.language.implicitConversions

trait CustomMarshallers extends ErrorAccumulatingCirceSupport {

  import StandardCharsets.UTF_8

  import ContentTypes._

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

  implicit val InvalidNotificationConfigurationExceptionMarshallers: ToEntityMarshaller[InvalidNotificationConfigurationException] =
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
