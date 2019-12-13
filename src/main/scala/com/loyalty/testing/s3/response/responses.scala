package com.loyalty.testing.s3.response

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Path, Paths}
import java.time.{Instant, LocalDateTime}
import java.util.UUID

import akka.stream.IOResult
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.request.BucketVersioning

import scala.concurrent.Future
import scala.xml.Elem

case class BucketResponse(bucketName: String,
                          locationConstraint: String = defaultRegion,
                          maybeBucketVersioning: Option[BucketVersioning] = None)

case class PutObjectResult(key: String,
                           etag: String,
                           contentMd5: String,
                           contentLength: Long,
                           maybeVersionId: Option[String],
                           index: Int = 0,
                           prefix: String = "") // TODO: remove this

case class ObjectMeta(path: Path,
                      result: PutObjectResult,
                      lastModifiedDate: LocalDateTime = LocalDateTime.now(),
                      id: UUID = UUID.randomUUID())

case class GetObjectResponse(bucketName: String,
                             key: String,
                             eTag: String,
                             contentMd5: String,
                             contentLength: Long,
                             content: Source[ByteString, Future[IOResult]],
                             maybeVersionId: Option[String] = None)

case object DeleteObjectResponse

trait XmlResponse {
  def toXml: Elem
  def toByteString: ByteString = ByteString(toXml.toString().getBytes(UTF_8))
}

case class ListBucketResult(bucketName: String,
                            keyCount: Int,
                            maxKeys: Int,
                            maybePrefix: Option[String] = None,
                            isTruncated: Boolean = false,
                            contents: List[BucketContent])
  extends XmlResponse {
  override def toXml: Elem = {
    val prefixElem = maybePrefix match {
      case Some(prefix) => <Prefix>{prefix}</Prefix>
      case None => <Prefix/>
    }

    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Name>{bucketName}</Name>{prefixElem}<KeyCount>{keyCount}</KeyCount><MaxKeys>{maxKeys}</MaxKeys><EncodingType>url</EncodingType> <IsTruncated>{isTruncated}</IsTruncated>{contents.map(_.toXml)}</ListBucketResult>
  }
}

case class BucketContent(expand: Boolean,
                         key: String,
                         size: Long,
                         eTag: String,
                         lastModifiedDate: Instant = Instant.now(),
                         storageClass: String = "STANDARD") extends XmlResponse {
  override def toXml: Elem =
    if (expand || size > 0)
      <Contents><Key>{key}</Key><LastModified>{lastModifiedDate.toString}</LastModified><Size>{size}</Size><StorageClass>{storageClass}</StorageClass><ETag>"{eTag}"</ETag></Contents>
    else <CommonPrefixes><Prefix>{key}</Prefix></CommonPrefixes>
}

case class InitiateMultipartUploadResult(bucketName: String, key: String, uploadId: String) extends XmlResponse {
  override def toXml: Elem =
    <InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>{bucketName}</Bucket><Key>{key.decode}</Key><UploadId>{uploadId}</UploadId></InitiateMultipartUploadResult>
}

case class CopyObjectResult(eTag: String,
                            maybeVersionId: Option[String] = None,
                            maybeSourceVersionId: Option[String] = None,
                            lastModifiedDate: Instant = Instant.now())
  extends XmlResponse {
  override def toXml: Elem =
    <CopyObjectResult><LastModified>{lastModifiedDate.toString}</LastModified><ETag>"{eTag}"</ETag></CopyObjectResult>
}

case class CopyPartResult(eTag: String,
                          maybeVersionId: Option[String] = None,
                          maybeSourceVersionId: Option[String] = None,
                          lastModifiedDate: Instant = Instant.now()) extends XmlResponse {
  override def toXml: Elem =
    <CopyPartResult><LastModified>{lastModifiedDate.toString}</LastModified><ETag>"{eTag}"</ETag></CopyPartResult>
}

case class CompleteMultipartUploadResult(bucketName: String, key: String, eTag: String, contentLength: Long,
                                         versionId: Option[String] = None) extends XmlResponse {
  val location = s"http://s3.amazonaws.com/${Paths.get(bucketName, key.decode).toString}"

  override def toXml: Elem =
    <CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>{location}</Location><Bucket>{bucketName}</Bucket><Key>{key.decode}</Key><ETag>"{eTag}"</ETag></CompleteMultipartUploadResult>
}

object ErrorCodes {
  val NoSuchBucket: String = "NoSuchBucket"
  val BucketAlreadyExists: String = "BucketAlreadyExists"
  val NoSuchKey: String = "NoSuchKey"
  val NoSuchUpload: String = "NoSuchUpload"
  val InvalidPart = "InvalidPart"
  val InvalidPartOrder = "InvalidPartOrder"
  val InvalidArgument = "InvalidArgument"
  val InvalidRequest = "InvalidRequest"
  val InternalError = "InternalError"
}

sealed trait ErrorResponse extends Throwable with XmlResponse {
  val code: String
  val message: String
  val resource: String

  override def getMessage: String = message

  override def toXml: Elem =
    <Error xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Code>{code}</Code><Message>{message}</Message><Resource>{resource}</Resource></Error>
}

import com.loyalty.testing.s3.response.ErrorCodes._

case class BucketAlreadyExistsException(bucketName: String) extends ErrorResponse {
  override val code: String = BucketAlreadyExists
  override val message: String = "The specified bucket already exist"
  override val resource: String = bucketName
}

case class NoSuchBucketException(bucketName: String) extends ErrorResponse {
  override val code: String = NoSuchBucket
  override val message: String = "The specified bucket does not exist"
  override val resource: String = bucketName
}

case class NoSuchKeyException(bucketName: String, key: String) extends ErrorResponse {
  override val code: String = NoSuchKey
  override val message: String = "The resource you requested does not exist"
  override val resource: String = s"/$bucketName/$key"
}

case class NoSuchUploadException(bucketName: String, key: String) extends ErrorResponse {
  override val code: String = NoSuchUpload
  override val message: String =
    """The specified multipart upload does not exist.
      |The upload ID might be invalid, or the multipart
      |upload might have been aborted or completed.""".stripMargin.replaceAll(System.lineSeparator(), "")
  override val resource: String = s"/$bucketName/$key"
}

case class InvalidPartException(bucketName: String, key: String, partNumber: Int, uploadId: String) extends ErrorResponse {
  override val code: String = InvalidPart
  override val message: String =
    """
      |One or more of the specified parts could not be found. The part might not have been uploaded,
      |or the specified entity tag might not have matched the part's entity tag.""".stripMargin
      .replaceAll(System.lineSeparator(), "")
  override val resource: String = s"/$bucketName/${key.decode}?partNumber=$partNumber&uploadId=$uploadId"
}

case class InvalidPartOrderException(bucketName: String, key: String) extends ErrorResponse {
  override val code: String = InvalidPartOrder
  override val message: String =
    """
      |The list of parts was not in ascending order. The parts list must be specified in order by part number.
    """.stripMargin.replaceAll(System.lineSeparator(), "")
  override val resource: String = s"/$bucketName/$key"
}

case class InvalidNotificationConfigurationException(bucketName: String, override val message: String) extends ErrorResponse {
  override val code: String = InvalidArgument
  override val resource: String = bucketName
}

case class InvalidRequestException(bucketName: String, override val message: String) extends ErrorResponse {
  override val code: String = InvalidRequest
  override val resource: String = bucketName
}

case class InternalServiceException(override val resource: String) extends ErrorResponse {
  override val code: String = InternalError
  override val message: String = "We encountered an internal error. Please try again."
}
