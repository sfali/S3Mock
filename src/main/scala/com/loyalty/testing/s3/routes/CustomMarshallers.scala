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

  implicit val BucketAlreadyExistsUnmarshaller: FromEntityUnmarshaller[BucketAlreadyExistsResponse] =
    nodeSeqUnmarshaller(MediaTypes.`application/xml`, `application/octet-stream`) map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x => BucketAlreadyExistsResponse((x \ "Resource").text)
    }

  implicit val BucketNotEmptyUnmarshaller: FromEntityUnmarshaller[BucketNotEmptyResponse] =
    nodeSeqUnmarshaller(MediaTypes.`application/xml`, `application/octet-stream`) map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x => BucketNotEmptyResponse((x \ "Resource").text)
    }

  implicit val NoSuchBucketUnmarshaller: FromEntityUnmarshaller[NoSuchBucketResponse] =
    nodeSeqUnmarshaller(MediaTypes.`application/xml`, `application/octet-stream`) map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x => NoSuchBucketResponse((x \ "Resource").text)
    }

  implicit val NoSuchKeyUnmarshaller: FromEntityUnmarshaller[NoSuchKeyResponse] =
    nodeSeqUnmarshaller(MediaTypes.`application/xml`, `application/octet-stream`) map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x =>
        val resource = (x \ "Resource").text.drop(1) // drop starting '/'
        val indexOfSeparator = resource.indexOf('/')
        val bucketName = resource.substring(0, indexOfSeparator)
        val key = resource.substring(indexOfSeparator + 1)
        NoSuchKeyResponse(bucketName, key)
    }

  implicit val NoSuchVersionUnmarshaller: FromEntityUnmarshaller[NoSuchVersionResponse] =
    nodeSeqUnmarshaller(MediaTypes.`application/xml`, `application/octet-stream`) map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x => NoSuchVersionResponse(x)
    }

  implicit val CopyObjectResultUnmarshaller: FromEntityUnmarshaller[CopyObjectResult] =
    nodeSeqUnmarshaller(MediaTypes.`application/xml`, `application/octet-stream`) map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x =>
        val etag = (x \ "ETag").text.drop(1).dropRight(1)
        val lastModified = Instant.parse((x \ "LastModified").text)
        CopyObjectResult(etag, lastModifiedDate = lastModified)
    }

  implicit val CopyPartResultUnmarshaller: FromEntityUnmarshaller[CopyPartResult] =
    nodeSeqUnmarshaller(MediaTypes.`application/xml`, `application/octet-stream`) map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x =>
        val etag = (x \ "ETag").text.drop(1).dropRight(1)
        val lastModified = Instant.parse((x \ "LastModified").text)
        CopyPartResult(etag, lastModifiedDate = lastModified)
    }

  implicit val InitiateMultipartUploadResultUnmarshaller: FromEntityUnmarshaller[InitiateMultipartUploadResult] =
    nodeSeqUnmarshaller(MediaTypes.`application/xml`, `application/octet-stream`) map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x =>
        val bucketName = (x \ "Bucket").text
        val key = (x \ "Key").text
        val uploadId = (x \ "UploadId").text
        InitiateMultipartUploadResult(bucketName, key, uploadId)
    }

  implicit val DeleteResultUnmarshaller: FromEntityUnmarshaller[DeleteResult] =
    nodeSeqUnmarshaller(MediaTypes.`application/xml`, `application/octet-stream`) map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x =>
        val deleted = Option(x \ "Deleted").map(_.map(DeletedObject(_))).getOrElse(Seq.empty).toList
        val errors = Option(x \ "Error").map(_.map(DeleteError(_))).getOrElse(Seq.empty).toList
        DeleteResult(deleted, errors)
    }

  implicit val CompleteMultipartUploadResultUnmarshaller: FromEntityUnmarshaller[CompleteMultipartUploadResult] =
    nodeSeqUnmarshaller(MediaTypes.`application/xml`, `application/octet-stream`) map {
      case NodeSeq.Empty => throw Unmarshaller.NoContentException
      case x =>
        val bucketName = (x \ "Bucket").text
        val key = (x \ "Key").text
        val etag = (x \ "ETag").text.drop(1).dropRight(1)
        CompleteMultipartUploadResult(bucketName, key, etag, 0L)
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

  implicit val DeleteResultMarshallers: ToEntityMarshaller[DeleteResult] =
    xmlResponseMarshallers(`text/xml(UTF-8)`)

  implicit val CopyPartResultMarshallers: ToEntityMarshaller[CopyPartResult] =
    xmlResponseMarshallers(`text/xml(UTF-8)`)

  implicit val CompleteMultipartUploadResultMarshallers: ToEntityMarshaller[CompleteMultipartUploadResult] =
    xmlResponseMarshallers(`text/xml(UTF-8)`)

  implicit val ListBucketResultMarshallers: ToEntityMarshaller[ListBucketResult] =
    xmlResponseMarshallers(`text/xml(UTF-8)`)

  implicit val BucketAlreadyExistsMarshallers: ToEntityMarshaller[BucketAlreadyExistsResponse] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val BucketNotEmptyMarshallers: ToEntityMarshaller[BucketNotEmptyResponse] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val NoSuchBucketMarshallers: ToEntityMarshaller[NoSuchBucketResponse] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val NoSuchKeyMarshallers: ToEntityMarshaller[NoSuchKeyResponse] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val NoSuchVersionMarshallers: ToEntityMarshaller[NoSuchVersionResponse] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val NoSuchUploadMarshallers: ToEntityMarshaller[NoSuchUploadResponse] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val InvalidPartMarshallers: ToEntityMarshaller[InvalidPartResponse] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val InvalidPartNumberMarshallers: ToEntityMarshaller[InvalidPartNumberResponse] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val InvalidPartOrderMarshallers: ToEntityMarshaller[InvalidPartOrderResponse] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val InvalidNotificationConfigurationExceptionMarshallers: ToEntityMarshaller[InvalidNotificationConfigurationException] =
    xmlResponseMarshallers(`application/octet-stream`)

  implicit val InternalServiceMarshallers: ToEntityMarshaller[InternalServiceResponse] =
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

  implicit val DeleteResultResponse: ToResponseMarshaller[DeleteResult] =
    fromToEntityMarshaller[DeleteResult](OK)

  implicit val BucketAlreadyExistsResponseMarshaller: ToResponseMarshaller[BucketAlreadyExistsResponse] =
    fromToEntityMarshaller[BucketAlreadyExistsResponse](BadRequest)

  implicit val BucketNotEmptyResponseMarshaller: ToResponseMarshaller[BucketNotEmptyResponse] =
    fromToEntityMarshaller[BucketNotEmptyResponse](Conflict)

  implicit val NoSuchBucketResponseMarshaller: ToResponseMarshaller[NoSuchBucketResponse] =
    fromToEntityMarshaller[NoSuchBucketResponse](NotFound)

  implicit val NoSuchKeyResponseMarshaller: ToResponseMarshaller[NoSuchKeyResponse] =
    fromToEntityMarshaller[NoSuchKeyResponse](NotFound)

  implicit val NoSuchVersionResponseMarshaller: ToResponseMarshaller[NoSuchVersionResponse] =
    fromToEntityMarshaller[NoSuchVersionResponse](NotFound)

  implicit val NoSuchUploadResponseMarshaller: ToResponseMarshaller[NoSuchUploadResponse] =
    fromToEntityMarshaller[NoSuchUploadResponse](NotFound)

  implicit val InvalidPartOrderResponseMarshaller: ToResponseMarshaller[InvalidPartOrderResponse] =
    fromToEntityMarshaller[InvalidPartOrderResponse](BadRequest)

  implicit val InvalidPartResponseMarshaller: ToResponseMarshaller[InvalidPartResponse] =
    fromToEntityMarshaller[InvalidPartResponse](BadRequest)

  implicit val InvalidPartNumberResponseMarshaller: ToResponseMarshaller[InvalidPartNumberResponse] =
    fromToEntityMarshaller[InvalidPartNumberResponse](BadRequest)

  implicit val ListBucketResultResponse: ToResponseMarshaller[ListBucketResult] =
    fromToEntityMarshaller[ListBucketResult](OK)

  implicit val InvalidNotificationConfigurationExceptionResponse: ToResponseMarshaller[InvalidNotificationConfigurationException] =
    fromToEntityMarshaller[InvalidNotificationConfigurationException](BadRequest)

  implicit val InternalServiceExceptionResponse: ToResponseMarshaller[InternalServiceResponse] =
    fromToEntityMarshaller[InternalServiceResponse](InternalServerError)

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
          (SourceVersionIdHeader, result.maybeSourceVersionId) +
          (VersionIdHeader, result.maybeVersionId)
        (OK, headers, result)
      }

  implicit val CopyPartResultResponse: ToResponseMarshaller[CopyPartResult] =
    fromStatusCodeAndHeadersAndValue[CopyPartResult]
      .compose { result =>
        val headers = Nil +
          (SourceVersionIdHeader, result.maybeSourceVersionId) +
          (VersionIdHeader, result.maybeVersionId)
        (OK, headers, result)
      }
}
