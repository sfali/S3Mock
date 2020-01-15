package com.loyalty.testing.s3.response

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant

import akka.util.ByteString
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.repositories.model.ObjectKey

import scala.xml.{Elem, NodeSeq}

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
    // @formatter:off
    val prefixElem = maybePrefix match {
      case Some(prefix) => <Prefix>{prefix}</Prefix>
      case None => <Prefix/>
    }
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>{bucketName}</Name>{prefixElem}<KeyCount>{keyCount}</KeyCount><MaxKeys>{maxKeys}</MaxKeys><EncodingType>url</EncodingType> <IsTruncated>{isTruncated}</IsTruncated>{contents.map(_.toXml)}</ListBucketResult>
    // @formatter:on
  }
}

case class BucketContent(expand: Boolean,
                         key: String,
                         size: Long,
                         eTag: String,
                         lastModifiedDate: Instant = Instant.now(),
                         storageClass: String = "STANDARD") extends XmlResponse {
  def fileName(delimiter: String = "/"): String = key.substring(key.lastIndexOf(delimiter) + 1)

  def prefix(delimiter: String = "/"): Option[String] = {
    val index = key.lastIndexOf(delimiter)
    if (index <= -1) None else Option(key.substring(0, index))
  }

  override def toXml: Elem =
  // @formatter:off
    if (expand || size > 0)
      <Contents><Key>{key}</Key><LastModified>{lastModifiedDate.toString}</LastModified><Size>{size}</Size><StorageClass>{storageClass}</StorageClass><ETag>"{eTag}"</ETag></Contents>
    else <CommonPrefixes><Prefix>{key}</Prefix></CommonPrefixes>
  // @formatter:on
}

object BucketContent {
  def apply(expand: Boolean,
            key: String,
            size: Long,
            eTag: String,
            lastModifiedDate: Instant = Instant.now(),
            storageClass: String = "STANDARD"): BucketContent =
    new BucketContent(expand, key, size, eTag, lastModifiedDate, storageClass)

  def apply(objectKey: ObjectKey): BucketContent =
    BucketContent(
      expand = true,
      objectKey.key,
      objectKey.contentLength,
      objectKey.eTag.getOrElse(""),
      objectKey.lastModifiedTime.toInstant
    )
}

case class InitiateMultipartUploadResult(bucketName: String, key: String, uploadId: String) extends XmlResponse {
  override def toXml: Elem =
  // @formatter:off
    <InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>{bucketName}</Bucket><Key>{key.decode}</Key><UploadId>{uploadId}</UploadId></InitiateMultipartUploadResult>
    // @formatter:on
}

case class CopyObjectResult(eTag: String,
                            maybeVersionId: Option[String] = None,
                            maybeSourceVersionId: Option[String] = None,
                            lastModifiedDate: Instant = Instant.now())
  extends XmlResponse {
  override def toXml: Elem =
  // @formatter:off
    <CopyObjectResult><LastModified>{lastModifiedDate.toString}</LastModified><ETag>"{eTag}"</ETag></CopyObjectResult>
  // @formatter:on
}

case class CopyPartResult(eTag: String,
                          maybeVersionId: Option[String] = None,
                          maybeSourceVersionId: Option[String] = None,
                          lastModifiedDate: Instant = Instant.now()) extends XmlResponse {
  override def toXml: Elem =
  // @formatter:off
    <CopyPartResult><LastModified>{lastModifiedDate.toString}</LastModified><ETag>"{eTag}"</ETag></CopyPartResult>
  // @formatter:on
}

case class CompleteMultipartUploadResult(bucketName: String,
                                         key: String,
                                         eTag: String,
                                         contentLength: Long,
                                         versionId: Option[String] = None) extends XmlResponse {
  val location = s"http://s3.amazonaws.com/$bucketName/${key.decode}.toString}"

  override def toXml: Elem =
  // @formatter:off
    <CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>{location}</Location><Bucket>{bucketName}</Bucket><Key>{key.decode}</Key><ETag>"{eTag}"</ETag></CompleteMultipartUploadResult>
  // @formatter:on
}

case class DeletedObject(key: String,
                         versionId: Option[String] = None,
                         deleteMarker: Option[Boolean] = None,
                         deleteMarkerVersionId: Option[String] = None) extends XmlResponse {
  override def toXml: Elem = {

    val keyElem = <Key>{key}</Key>
    (versionId, deleteMarker, deleteMarkerVersionId) match {
      case (None, None, None) =>
        // @formatter:off
        <Deleted>{keyElem}</Deleted>
        // @formatter:on
      case (Some(value1), None, None) =>
        // @formatter:off
        <Deleted>{keyElem}<VersionId>{value1}</VersionId></Deleted>
        // @formatter:on
      case (None, Some(value2), None) =>
        // @formatter:off
        <Deleted>{keyElem}<DeleteMarker>{value2}</DeleteMarker></Deleted>
        // @formatter:on
      case (None, None, Some(value3)) =>
        // @formatter:off
        <Deleted>{keyElem}<DeleteMarkerVersionId>{value3}</DeleteMarkerVersionId></Deleted>
       // @formatter:on
      case (Some(value1), Some(value2), None) =>
        // @formatter:off
        <Deleted>{keyElem}<VersionId>{value1}</VersionId><DeleteMarker>{value2}</DeleteMarker></Deleted>
        // @formatter:on
      case (Some(value1), None, Some(value3)) =>
        // @formatter:off
        <Deleted>{keyElem}<VersionId>{value1}</VersionId><DeleteMarkerVersionId>{value3}</DeleteMarkerVersionId></Deleted>
        // @formatter:on
      case (Some(value1), Some(value2), Some(value3)) =>
        // @formatter:off
        <Deleted>{keyElem}<VersionId>{value1}</VersionId><DeleteMarker>{value2}</DeleteMarker><DeleteMarkerVersionId>{value3}</DeleteMarkerVersionId></Deleted>
        // @formatter:on
      case (None, Some(value2), Some(value3)) =>
        // @formatter:off
        <Deleted>{keyElem}<DeleteMarker>{value2}</DeleteMarker><DeleteMarkerVersionId>{value3}</DeleteMarkerVersionId></Deleted>
        // @formatter:on
    }
  }
}

object DeletedObject {
  def apply(key: String,
            versionId: Option[String] = None,
            deleteMarker: Option[Boolean] = None,
            deleteMarkerVersionId: Option[String] = None): DeletedObject =
    new DeletedObject(key, versionId, deleteMarker, deleteMarkerVersionId)

  def apply(nodeSeq: NodeSeq): DeletedObject = {
    val key = Option(nodeSeq \ "Key").map(_.text.trim).getOrElse("")
    val versionId = Option(nodeSeq \ "VersionId").map(_.text.trim).flatMap(_.toOption)
    val deleteMarker = Option(nodeSeq \ "DeleteMarker").map(_.text.trim).flatMap(_.toBooleanOption)
    val deleteMarkerVersionId = Option(nodeSeq \ "DeleteMarkerVersionId").map(_.text.trim).flatMap(_.toOption)
    DeletedObject(key, versionId, deleteMarker, deleteMarkerVersionId)
  }
}

case class DeleteError(key: String, code: String, message: String) extends XmlResponse {
  override def toXml: Elem =
  // @formatter:off
    <Error><Key>{key}</Key><Code>{code}</Code><Message>{message}</Message></Error>
  // @formatter:on
}

object DeleteError {
  def apply(key: String, code: String, message: String): DeleteError = new DeleteError(key, code, message)

  def apply(nodeSeq: NodeSeq): DeleteError = {
    val key = Option(nodeSeq \ "Key").map(_.text.trim).getOrElse("")
    val code = Option(nodeSeq \ "Code").map(_.text.trim).getOrElse("")
    val message = Option(nodeSeq \ "Message").map(_.text.trim).getOrElse("")
    DeleteError(key, code, message)
  }
}

case class DeleteResult(deleted: List[DeletedObject] = Nil,
                        errors: List[DeleteError] = Nil) extends XmlResponse {
  override def toXml: Elem =
  // @formatter:off
    <DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{deleted.map(_.toXml)}{errors.map(_.toXml)}</DeleteResult>
    // @formatter:on
}

object ErrorCodes {
  val NoSuchBucket: String = "NoSuchBucket"
  val BucketAlreadyExists: String = "BucketAlreadyExists"
  val NoSuchKey: String = "NoSuchKey"
  val NoSuchUpload: String = "NoSuchUpload"
  val InvalidPart = "InvalidPart"
  val InvalidPartNumber = "InvalidPartNumber"
  val InvalidPartOrder = "InvalidPartOrder"
  val InvalidArgument = "InvalidArgument"
  val InvalidRequest = "InvalidRequest"
  val InternalError = "InternalError"
}

sealed trait ErrorResponse extends XmlResponse {
  val code: String
  val message: String
  val resource: String

  override def toXml: Elem =
  // @formatter:off
    <Error xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Code>{code}</Code><Message>{message}</Message><Resource>{resource}</Resource></Error>
  // @formatter:on
}

import com.loyalty.testing.s3.response.ErrorCodes._

case class BucketAlreadyExistsResponse(bucketName: String) extends ErrorResponse {
  override val code: String = BucketAlreadyExists
  override val message: String = "The specified bucket already exist"
  override val resource: String = bucketName
}

case class BucketNotEmptyResponse(bucketName: String) extends ErrorResponse {
  override val code: String = BucketAlreadyExists
  override val message: String = "The bucket you tried to delete is not empty"
  override val resource: String = bucketName
}

case class NoSuchBucketResponse(bucketName: String) extends ErrorResponse {
  override val code: String = NoSuchBucket
  override val message: String = "The specified bucket does not exist"
  override val resource: String = bucketName
}

case class NoSuchKeyResponse(bucketName: String, key: String) extends ErrorResponse {
  override val code: String = NoSuchKey
  override val message: String = "The resource you requested does not exist"
  override val resource: String = s"/$bucketName/$key"
}

case class NoSuchUploadResponse(bucketName: String, key: String) extends ErrorResponse {
  override val code: String = NoSuchUpload
  override val message: String =
    """The specified multipart upload does not exist.
      |The upload ID might be invalid, or the multipart
      |upload might have been aborted or completed.""".stripMargin.replaceAll(System.lineSeparator(), "")
  override val resource: String = s"/$bucketName/$key"
}

case class InvalidPartResponse(bucketName: String, key: String, partNumber: Int, uploadId: String) extends ErrorResponse {
  override val code: String = InvalidPart
  override val message: String =
    """
      |One or more of the specified parts could not be found. The part might not have been uploaded,
      |or the specified entity tag might not have matched the part's entity tag.""".stripMargin
      .replaceAll(System.lineSeparator(), "")
  override val resource: String = s"/$bucketName/${key.decode}?partNumber=$partNumber&uploadId=$uploadId"
}

case class InvalidPartNumberResponse(bucketName: String, key: String, partNumber: Int) extends ErrorResponse {
  override val code: String = InvalidPartNumber
  override val message: String = "The requested partnumber is not satisfiable"
  override val resource: String = s"/$bucketName/${key.decode}?partNumber=$partNumber"
}

case class InvalidPartOrderResponse(bucketName: String, key: String) extends ErrorResponse {
  override val code: String = InvalidPartOrder
  override val message: String =
    """
      |The list of parts was not in ascending order. The parts list must be specified in order by part number.
    """.stripMargin.replaceAll(System.lineSeparator(), "")
  override val resource: String = s"/$bucketName/$key"
}

case class InvalidNotificationConfigurationException(bucketName: String, override val message: String)
  extends Throwable with ErrorResponse {
  override val code: String = InvalidArgument
  override val resource: String = bucketName
}

case class InvalidRequestException(bucketName: String, override val message: String) extends ErrorResponse {
  override val code: String = InvalidRequest
  override val resource: String = bucketName
}

case class InternalServiceResponse(override val resource: String) extends ErrorResponse {
  override val code: String = InternalError
  override val message: String = "We encountered an internal error. Please try again."
}
