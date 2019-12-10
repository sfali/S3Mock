package com.loyalty.testing.s3.repositories

import java.nio.file.{Files, Path}
import java.time.OffsetDateTime
import java.util.UUID
import java.util.stream.Collectors

import akka.Done
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.IOResult
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.repositories.model.{Bucket, ObjectKey, UploadInfo}
import com.loyalty.testing.s3.request.{BucketVersioning, PartInfo}
import com.loyalty.testing.s3.streams.FileStream

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class ObjectIO(root: Path, fileStream: FileStream) {

  private val workDir: Path = root.toAbsolutePath
  private val dataDir: Path = workDir + "data"
  private val uploadsStagingDir: Path = workDir + "uploads-staging"
  private val uploadsDir: Path = workDir + "uploads"

  def saveObject(bucket: Bucket,
                 key: String,
                 keyId: UUID,
                 versionIndex: Int,
                 contentSource: Source[ByteString, _])
                (implicit ec: ExecutionContext): Future[ObjectKey] = {
    val versionId = versionIndex.toVersionId
    val objectPath = getObjectPath(bucket.bucketName, key, bucket.version, versionId)
    fileStream.saveContent(contentSource, objectPath)
      .flatMap {
        digestInfo =>
          if (Files.notExists(objectPath)) Future.failed(new RuntimeException("unable to save file"))
          else
            Future.successful(ObjectKey(
              id = keyId,
              bucketName = bucket.bucketName,
              key = key,
              index = versionIndex,
              version = bucket.version,
              versionId = versionId,
              eTag = digestInfo.etag,
              contentMd5 = digestInfo.md5,
              contentLength = digestInfo.length,
              lastModifiedTime = OffsetDateTime.now()
            ))
      }
  }

  def savePart(uploadInfo: UploadInfo, contentSource: Source[ByteString, _])
              (implicit ec: ExecutionContext): Future[UploadInfo] = {
    val objectPath = getUploadPath(uploadInfo, staging = true)
    fileStream.saveContent(contentSource, objectPath)
      .flatMap {
        digestInfo =>
          if (Files.notExists(objectPath)) Future.failed(new RuntimeException("unable to save file"))
          else {
            val updateUploadInfo = uploadInfo.copy(
              eTag = digestInfo.etag,
              contentMd5 = digestInfo.md5,
              contentLength = digestInfo.length
            )
            Future.successful(updateUploadInfo)
          }
      }
  }

  def mergeFiles(uploadInfo: UploadInfo, parts: List[PartInfo])
                (implicit ec: ExecutionContext): Future[ObjectKey] = {
    val versionId = uploadInfo.versionIndex.toVersionId
    val objectPath = getObjectPath(uploadInfo.bucketName, uploadInfo.key, uploadInfo.version, versionId)
    val partPaths = parts.map(partInfo => uploadInfo.copy(partNumber = partInfo.partNumber))
      .map(uploadInfo => getUploadPath(uploadInfo, staging = true))

    val concatenatedETag =
      parts
        .map(_.eTag)
        .foldLeft("") {
          case (agg, etag) => agg + etag
        }
    val finalETag = s"${toBase16(concatenatedETag)}-${parts.length}"
    fileStream.mergeFiles(objectPath, partPaths)
      .flatMap {
        digestInfo =>
          if (Files.notExists(objectPath)) Future.failed(new RuntimeException("unable to save file"))
          else
            Future.successful(ObjectKey(
              id = createObjectId(uploadInfo.bucketName, uploadInfo.key),
              bucketName = uploadInfo.bucketName,
              key = uploadInfo.key,
              index = uploadInfo.versionIndex,
              version = uploadInfo.version,
              versionId = versionId,
              eTag = finalETag,
              contentMd5 = digestInfo.md5,
              contentLength = digestInfo.length,
              lastModifiedTime = OffsetDateTime.now(),
              uploadId = Some(uploadInfo.uploadId)
            ))
      }
  }

  def moveParts(objectKey: ObjectKey, uploadInfo: UploadInfo): Future[Done] = {
    val stagingPath = getUploadPath(uploadInfo, staging = true)
    val path = getUploadPath(uploadInfo, staging = false)
    Try {
      if (BucketVersioning.Enabled != objectKey.version) clean(path) // first clean existing folder, if applicable
      Files.move(stagingPath, path)
      clean(getUploadPath(uploadInfo, staging = true))
    } match {
      case Failure(ex) => Future.failed(ex)
      case Success(_) => Future.successful(Done)
    }
  }

  def getObject(objectKey: ObjectKey,
                maybeRange: Option[ByteRange] = None): (ObjectKey, Source[ByteString, Future[IOResult]]) = {
    val objectPath = getObjectPath(objectKey.bucketName, objectKey.key, objectKey.version, objectKey.versionId)
    val (downloadRange, source) = fileStream.downloadFile(objectPath, maybeRange = maybeRange)
    (objectKey.copy(contentLength = downloadRange.capacity), source)
  }

  def delete(objectKey: ObjectKey): Unit = {
    val objectPath = getObjectPath(objectKey.bucketName, objectKey.key, objectKey.version, objectKey.versionId)
    val path = objectPath.getParent.getParent
    val parent = path.getParent
    clean(path)
    val empty = Files.list(parent).collect(Collectors.toList()).isEmpty
    if (empty) clean(parent)
  }

  def initiateMultipartUpload(uploadInfo: UploadInfo): Path = getUploadPath(uploadInfo, staging = true)

  private def getObjectPath(bucketName: String,
                            key: String,
                            bucketVersioning: BucketVersioning,
                            versionId: String) = {
    val objectParentPath = dataDir + (bucketName, key, toBase16(bucketVersioning.entryName), versionId)
    objectParentPath -> ContentFileName
  }

  private def getUploadPath(uploadInfo: UploadInfo, staging: Boolean) = {
    val path = if (staging) uploadsStagingDir else uploadsDir
    val uploadPath = path + (uploadInfo.bucketName, uploadInfo.key, uploadInfo.uploadId,
      toBase16(uploadInfo.version.entryName), uploadInfo.versionIndex.toVersionId)
    if (uploadInfo.partNumber > 0) {
      val partPath = uploadPath + uploadInfo.partNumber.toString
      partPath -> ContentFileName
    } else uploadPath

  }

}

object ObjectIO {
  def apply(root: Path, fileStream: FileStream): ObjectIO = new ObjectIO(root, fileStream)
}
