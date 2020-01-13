package com.loyalty.testing.s3.service

import java.util.UUID

import akka.Done
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.IOResult
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.repositories.model.{Bucket, ObjectKey, ObjectStatus, UploadInfo}
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.request.PartInfo
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class ObjectService(objectIO: ObjectIO, database: NitriteDatabase) {

  private val log = LoggerFactory.getLogger(classOf[ObjectService])

  def getAllObjects(objectId: UUID)(implicit ec: ExecutionContext): Future[(List[ObjectKey], List[UploadInfo])] =
    for {
      objects <- database.getAllObjects(objectId)
      uploads <- database.findUploads
    } yield (objects, uploads)

  def saveObject(bucket: Bucket,
                 key: String,
                 keyId: UUID,
                 versionIndex: Int,
                 contentSource: Source[ByteString, _])
                (implicit ec: ExecutionContext): Future[ObjectKey] =
    for {
      sourceObjectKey <- objectIO.saveObject(bucket, key, keyId, versionIndex, contentSource)
      finalObjectKey <- createOrUpdateObject(sourceObjectKey)
    } yield finalObjectKey

  def createOrUpdateObject(objectKey: ObjectKey): Future[ObjectKey] = database.createOrUpdateObject(objectKey)

  def getObject(objectKey: ObjectKey,
                maybePartNumber: Option[Int] = None,
                maybeRange: Option[ByteRange] = None): (ObjectKey, Source[ByteString, Future[IOResult]]) = {
    val (actualObjectKey, partNumber) = database.getObject(objectKey, maybePartNumber)
    objectIO.getObject(actualObjectKey, partNumber, maybeRange)
  }

  def virtualDeleteObject(objectKey: ObjectKey)
                         (implicit ec: ExecutionContext): Future[ObjectKey] = {
    val maybeUploadId = objectKey.uploadId
    if (objectKey.isDeleted) Future.failed(NoSuchKeyException)
    else {
      val (status, deleteMarker) = objectKey.status match {
        case ObjectStatus.Active => (ObjectStatus.Deleted, None)
        case ObjectStatus.DeleteMarker => (ObjectStatus.DeleteMarkerDeleted, Some(true))
        case s => throw new RuntimeException(s"Invalid object status: ${objectKey.bucketName}/${objectKey.key}/$s")
      }
      val updatedObjectKey = objectKey.copy(eTag = None, contentMd5 = None, contentLength = 0L, status = status,
        uploadId = None, objectPath = None, deleteMarker = deleteMarker)
      for {
        ok <- database.createOrUpdateObject(updatedObjectKey)
        _ <- deleteFile(objectKey)
        _ <- database.deleteUpload(maybeUploadId)
      } yield ok
    }
  }

  def createUpload(uploadInfo: UploadInfo)
                  (implicit ec: ExecutionContext): Future[Done] =
    database.createUpload(uploadInfo)
      .map(_ => objectIO.initiateMultipartUpload(uploadInfo))
      .map(_ => Done)

  def savePart(partInfo: UploadInfo, contentSource: Source[ByteString, _])
              (implicit ec: ExecutionContext): Future[UploadInfo] =
    for {
      maybeUploadInfo <- objectIO.savePart(partInfo, contentSource)
      if maybeUploadInfo.isDefined
      uploadInfo = maybeUploadInfo.get
      _ <- database.createUpload(uploadInfo)
    } yield uploadInfo

  def completeUpload(uploadInfo: UploadInfo,
                     parts: List[PartInfo],
                     uploadParts: Map[String, Set[UploadInfo]])
                    (implicit ec: ExecutionContext): Future[ObjectKey] =
    if (checkPartOrders(parts.map(_.partNumber))) {
      val savedParts =
        uploadParts.get(uploadInfo.uploadId) match {
          case Some(values) => values.map(PartInfo(_)).toList.sortBy(_.partNumber)
          case None => Nil
        }
      if (savedParts.isEmpty) Future.failed(InternalServiceException)
      else if (savedParts != parts) {
        val diff = savedParts.diff(parts)
        log.warn("Invalid part order: bucket={}, key={}, upload_id={}, saved_parts={}, request_parts={}",
          uploadInfo.bucketName, uploadInfo.key, uploadInfo.uploadId, savedParts, parts)
        Future.failed(InvalidPartException(diff.head.partNumber))
      } else {
        for {
          objectKey <- objectIO.mergeFiles(uploadInfo, parts)
          savedObjectKey <- database.createOrUpdateObject(objectKey)
          _ <- objectIO.moveParts(savedObjectKey, uploadInfo)
          _ <- database.moveParts(savedObjectKey)
        } yield savedObjectKey
      }
    } else Future.failed(InvalidPartOrderException)

  def deleteUpload(uploadId: String, partNumber: Int): Future[Done] = database.deleteUpload(uploadId, partNumber)


  private def deleteFile(objectKey: ObjectKey): Future[Done] =
    Try(objectIO.delete(objectKey)) match {
      case Failure(ex) =>
        // should we penalize client if we are unable to delete file, let's consume exception,
        // log and move on
        log.warn(
          s"""unable to delete file, bucket_name=${objectKey.bucketName}, key=${objectKey.key},
             | version_id=${objectKey.actualVersionId}""".stripMargin.replaceNewLine, ex)
        Future.successful(Done)
      case Success(_) => Future.successful(Done)
    }
}

object ObjectService {
  def apply(objectIO: ObjectIO, database: NitriteDatabase): ObjectService =
    new ObjectService(objectIO, database)
}

case object NoSuchKeyException extends Exception("NoSuchKeyException")

case object InvalidPartOrderException extends Exception("InvalidPartOrder")

case class InvalidPartException(partNumber: Int) extends Exception(s"InvalidPart: $partNumber")

case object InternalServiceException extends Exception("InternalError")
