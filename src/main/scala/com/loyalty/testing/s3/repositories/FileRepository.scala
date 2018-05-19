package com.loyalty.testing.s3.repositories

import java.nio.file._

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.headers.ByteRange
import akka.http.scaladsl.model.headers.ByteRange.{FromOffset, Slice, Suffix}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.loyalty.testing.s3.request.BucketVersioning.BucketVersioning
import com.loyalty.testing.s3.request._
import com.loyalty.testing.s3.response._
import com.loyalty.testing.s3.streams.{FileStream, RangeDownloadSource}

import scala.concurrent.Future

class FileRepository(fileStore: FileStore, fileStream: FileStream, log: LoggingAdapter)
                    (implicit mat: ActorMaterializer) extends Repository {

  import FileRepository._
  import com.loyalty.testing.s3._
  import mat.executionContext

  override def createBucketWithVersioning(bucketName: String,
                                          bucketConfiguration: CreateBucketConfiguration,
                                          maybeBucketVersioning: Option[BucketVersioning]): Future[BucketResponse] = {
    maybeBucketVersioning match {
      case Some(bucketVersioning) =>
        for {
          bucketResponse <- createBucket(bucketName, bucketConfiguration)
          _ <- setBucketVersioning(bucketName, VersioningConfiguration(bucketVersioning))
        } yield bucketResponse
      case None => createBucket(bucketName, bucketConfiguration)
    }
  }

  override def createBucket(bucketName: String,
                            bucketConfiguration: CreateBucketConfiguration): Future[BucketResponse] =
    fileStore.get(bucketName) match {
      case None =>
        Future.successful {
          val bucketPath = fileStore.dataDir + bucketName
          log.info("Bucket created: {}", bucketPath.toString)
          val metaData = BucketMetadata(bucketName, bucketPath)
          metaData.location = bucketConfiguration.locationConstraint
          fileStore.add(bucketName, metaData)
          BucketResponse(metaData.bucketName, bucketConfiguration.locationConstraint)
        }
      case Some(_) => Future.failed(BucketAlreadyExistsException(bucketName))
    }

  override def setBucketVersioning(bucketName: String,
                                   versioningConfiguration: VersioningConfiguration): Future[BucketResponse] =
    fileStore.get(bucketName) match {
      case None => Future.failed(NoSuchBucketException(bucketName))
      case Some(bucketMetaData) =>
        Future.successful {
          log.info("Setting versioning of bucket {} to: {}", bucketName,
            versioningConfiguration.bucketVersioning)
          bucketMetaData.maybeBucketVersioning = versioningConfiguration
          BucketResponse(bucketName, bucketMetaData.location,
            bucketMetaData.maybeBucketVersioning.map(_.bucketVersioning))
        }
    }

  override def putObject(bucketName: String, key: String, contentSource: Source[ByteString, _]): Future[ObjectMeta] =
    fileStore.get(bucketName) match {
      case None => Future.failed(NoSuchBucketException(bucketName))
      case Some(bucketMetadata) =>
        val (maybeVersionId, filePath) = getDestinationPathWithVersionId(key, bucketMetadata)
        fileStream.saveContent(contentSource, filePath)
          .flatMap {
            digest =>
              if (Files.notExists(filePath)) Future.failed(new RuntimeException("unable to save file"))
              else {
                val response = ObjectMeta(filePath,
                  createPutObjectResult(digest, digest, Files.size(filePath), maybeVersionId))
                bucketMetadata.putObject(key, response)
                Future.successful(response)
              }
          }
    }

  private def getDestinationPathWithVersionId(key: String, bucketMetadata: BucketMetadata): (Option[String], Path) = {
    val keyPath = bucketMetadata.path -> key
    val fileName = keyPath.getFileName.toString
    val parentPath = keyPath.getParent

    val maybeVersioningConfiguration = bucketMetadata.maybeBucketVersioning
      .filter(_.bucketVersioning == BucketVersioning.Enabled)
    val (maybeVersionId, filePath) =
      maybeVersioningConfiguration match {
        case Some(_) =>
          val versionId = md5HexFromRandomUUID
          (Some(versionId), parentPath -> (versionId, fileName))
        case None =>
          (None, parentPath -> (NonVersionId, fileName))
      }
    Files.createDirectories(filePath.getParent)
    (maybeVersionId, filePath)
  }

  override def getObject(bucketName: String, key: String, maybeVersionId: Option[String] = None,
                         maybeRange: Option[ByteRange] = None): Future[GetObjectResponse] =
    fileStore.get(bucketName) match {
      case None => Future.failed(NoSuchBucketException(bucketName))
      case Some(bucketMetadata) =>
        val objectPath = getObjectPath(bucketMetadata, key, maybeVersionId)
        val maybeObj = bucketMetadata.getObject(key)
        if (maybeObj.isEmpty || Files.notExists(objectPath)) Future.failed(NoSuchKeyException(bucketName, key))
        else {
          val meta = maybeObj.get
          val contentSource = RangeDownloadSource.fromPath(meta.path, maybeRange = maybeRange)
          Future.successful(GetObjectResponse(bucketName, key, meta.result.getETag, meta.result.getContentMd5,
            contentSource, Option(meta.result.getVersionId)))
        }
    }

  private def getRange(contentLength: Long, maybeRange: Option[ByteRange]): Option[Slice] =
    maybeRange.flatMap {
      case Slice(first, last) => Some(Slice(first, last))
      case FromOffset(offset) => Some(Slice(offset, contentLength - 1))
      case Suffix(length) => Some(Slice(contentLength - length, contentLength - 1))
    }

  private def getObjectPath(bucketMetadata: BucketMetadata, key: String,
                            maybeVersionId: Option[String] = None): Path = {
    val keyPath = bucketMetadata.path -> key
    val fileName = keyPath.getFileName.toString
    val parentPath = keyPath.getParent

    maybeVersionId.map(versionId => parentPath -> (versionId, fileName))
      .getOrElse(bucketMetadata.getObject(key).get.path)
  }

  def deleteObject(bucketName: String, key: String,
                   maybeVersionId: Option[String] = None): Future[DeleteObjectResponse.type] =
    fileStore.get(bucketName) match {
      case None => Future.failed(NoSuchBucketException(bucketName))
      case Some(bucketMetadata) =>
        val objectPath = getObjectPath(bucketMetadata, key, maybeVersionId)
        val maybeObj = bucketMetadata.getObject(key)
        if (maybeObj.isEmpty || Files.notExists(objectPath)) Future.failed(NoSuchKeyException(bucketName, key))
        else {
          fileStore.removeObject(bucketName, key)
          fileStore.clean(objectPath.getParent)
          Future.successful(DeleteObjectResponse)
        }
    }

  override def initiateMultipartUpload(bucketName: String, key: String): Future[InitiateMultipartUploadResult] =
    fileStore.get(bucketName) match {
      case None => Future.failed(NoSuchBucketException(bucketName))
      case Some(_) =>
        Future.successful {
          val uploadId = md5HexFromRandomUUID
          val result = InitiateMultipartUploadResult(bucketName, key, uploadId)
          val uploadDir = fileStore.uploadsDir + (bucketName, key, uploadId)
          log.info("Upload dir created @ {}", uploadDir)
          result
        }
    }

  override def uploadMultipart(bucketName: String, key: String, partNumber: Int, uploadId: String,
                               contentSource: Source[ByteString, _]): Future[ObjectMeta] =
    fileStore.get(bucketName) match {
      case None => Future.failed(NoSuchBucketException(bucketName))
      case Some(bucketMetadata) =>
        val uploadPath = fileStore.uploadsDir -> (bucketName, key, uploadId)
        if (Files.notExists(uploadPath)) Future.failed(NoSuchUploadException(bucketName, key))
        else {
          val filePath = uploadPath -> partNumber.toString
          Files.createDirectories(filePath.getParent)
          fileStream.saveContent(contentSource, filePath)
            .flatMap {
              digest =>
                if (Files.notExists(filePath)) Future.failed(new RuntimeException("unable to save file"))
                else {
                  val eTag = digest
                  bucketMetadata.addPart(uploadId, UploadPart(partNumber, eTag))
                  val response = ObjectMeta(filePath, createPutObjectResult(eTag, digest, Files.size(filePath)))
                  bucketMetadata.putObject(key, response)
                  Future.successful(response)
                }
            }
        }
    }

  override def completeMultipart(bucketName: String,
                                 key: String,
                                 uploadId: String,
                                 completeMultipartUpload: CompleteMultipartUpload): Future[CompleteMultipartUploadResult] = {
    fileStore.get(bucketName) match {
      case None => Future.failed(NoSuchBucketException(bucketName))
      case Some(bucketMetadata) =>
        val uploadPath = fileStore.uploadsDir -> (bucketName, key, uploadId)
        if (Files.notExists(uploadPath)) Future.failed(NoSuchUploadException(bucketName, key))
        else {
          if (checkPartOrder(completeMultipartUpload)) {
            val parts = completeMultipartUpload.parts
            val tuples = parts.map(part => (part.partNumber, uploadPath -> part.partNumber.toString))
            val invalidParts = tuples.filter(path => Files.notExists(path._2))
            if (invalidParts.nonEmpty)
              Future.failed(InvalidPartException(bucketName, key, invalidParts.head._1, uploadId))
            else {
              val (maybeVersionId, filePath) = getDestinationPathWithVersionId(key, bucketMetadata)
              val result = createCompleteMultipartUploadResult(bucketName, key, parts, maybeVersionId)
              fileStream.mergeFiles(filePath, tuples.map(_._2))
                .flatMap {
                  digest =>
                    if (Files.notExists(filePath)) Future.failed(new RuntimeException("unable to save file"))
                    else {
                      val eTag = digest
                      val contentLength = Files.size(filePath)
                      val response = ObjectMeta(filePath,
                        createPutObjectResult(eTag, digest, contentLength, maybeVersionId))
                      bucketMetadata.putObject(key, response)
                      Future.successful(result.copy(contentLength = contentLength))
                    }
                }
            }
          } else Future.failed(InvalidPartOrderException(bucketName, key))
        }
    }
  }

  override def copyMultipart(bucketName: String,
                             key: String,
                             partNumber: Int,
                             uploadId: String,
                             sourceBucketName: String,
                             sourceKey: String,
                             maybeSourceVersionId: Option[String] = None,
                             maybeSourceRange: Option[ByteRange] = None): Future[CopyPartResult] = {
    fileStore.get(sourceBucketName) match {
      case None => Future.failed(NoSuchBucketException(bucketName))
      case Some(bucketMetadata) =>
        val objectPath = getObjectPath(bucketMetadata, sourceKey, maybeSourceVersionId)
        val maybeObj = bucketMetadata.getObject(sourceKey)
        val uploadPath = fileStore.uploadsDir -> (bucketName, key, uploadId)
        if (maybeObj.isEmpty || Files.notExists(objectPath)) Future.failed(NoSuchKeyException(bucketName, key))
        else if (Files.notExists(uploadPath)) Future.failed(NoSuchUploadException(bucketName, key))
        else {
          val meta = maybeObj.get
          val contentLength = Files.size(meta.path)
          val slice = getRange(contentLength, maybeSourceRange)
          val destinationPath = uploadPath -> partNumber.toString
          Files.createDirectories(destinationPath.getParent)
          fileStream.copyPart(meta.path, destinationPath, slice)
            .map {
              eTag => CopyPartResult(eTag)
            }
        }
    }
  }

  override def clean(): Unit = fileStore.clean

  private def checkPartOrder(completeMultipartUpload: CompleteMultipartUpload): Boolean = {
    val partNumbers = completeMultipartUpload.parts.map(_.partNumber)
    if (partNumbers.isEmpty || partNumbers.head != 1 || partNumbers.last != partNumbers.length)
      false
    else {
      val partsSum = partNumbers.sum
      val n = partNumbers.last
      val sum = (n * (n + 1)) / 2 // sum of n numbers
      partsSum == sum
    }
  }
}

object FileRepository {

  private val NonVersionId = "null"

  def apply(fileStore: FileStore, log: LoggingAdapter)(implicit mat: ActorMaterializer): FileRepository =
    new FileRepository(fileStore, FileStream(), log)
}
