package com.loyalty.testing.s3.repositories

import java.nio.file._

import akka.Done
import akka.event.LoggingAdapter
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.loyalty.testing.s3.notification.Notification
import com.loyalty.testing.s3.repositories.FileStore
import com.loyalty.testing.s3.request._
import com.loyalty.testing.s3.response._
import com.loyalty.testing.s3.streams.FileStream

import scala.concurrent.Future
import scala.util.{Failure, Success}

class FileRepository(fileStore: FileStore, fileStream: FileStream, log: LoggingAdapter)
                    (implicit mat: Materializer) extends Repository {

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

  override def setBucketVersioning(bucketName: String, contentSource: Source[ByteString, _]): Future[BucketResponse] =
    toVersionConfiguration(contentSource)
      .flatMap {
        versioningConfiguration => setBucketVersioning(bucketName, versioningConfiguration)
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

  override def setBucketNotification(bucketName: String, contentSource: Source[ByteString, _]): Future[Done] =
    toBucketNotification(bucketName, contentSource)
      .flatMap {
        notifications => setBucketNotification(bucketName, notifications)
      }

  override def setBucketNotification(bucketName: String, notifications: List[Notification]): Future[Done] =
    fileStore.get(bucketName) match {
      case None => Future.failed(NoSuchBucketException(bucketName))
      case Some(bucketMetadata) =>
        Future.successful {
          log.info("Setting bucket notification for: {}", bucketName)
          bucketMetadata.notifications = notifications
          fileStore.add(bucketName, bucketMetadata)
          Done
        }
    }

  override def listBucket(bucketName: String, params: ListBucketParams): Future[ListBucketResult] =
    fileStore.get(bucketName) match {
      case None => Future.failed(NoSuchBucketException(bucketName))
      case Some(bucketMetadata) => Future.successful {
        val objects = bucketMetadata.getObjects
        val recursive = params.maybeDelimiter.isEmpty
        val filteredObjects =
          (params.maybePrefix, params.maybeDelimiter) match {
            case (None, None) =>
              log.debug("Case 1: Prefix: None, Delimiter: None")
              objects
            case (Some(prefix), None) =>
              log.debug("Case 2: Prefix: {}, Delimiter: None", prefix)
              objects.filter {
                om =>
                  val result = om.result
                  result.key == prefix || result.prefix.startsWith(prefix)
              }
            case (None, Some(delimiter)) =>
              log.debug("Case 3: Prefix: None, Delimiter: {}", delimiter)
              objects.filter(_.result.prefix == "/")
            case (Some(prefix), Some(delimiter)) =>
              log.debug("Case 4: Prefix: {}, Delimiter: {}", prefix, delimiter)
              objects.filter {
                om =>
                  val result = om.result
                  result.key == prefix || result.prefix == prefix
              }
          }

        val _prefix = params.maybePrefix.getOrElse("")

        log.debug("FilteredObjects: {}", filteredObjects.map(_.result.key))
        val contents = filteredObjects
          .map {
            om =>
              val result = om.result
              val key = result.key
              val expand = recursive || key == _prefix
              BucketContent(expand = expand, key = key, eTag = result.etag, size = result.contentLength)
          }
          .sortBy(_.key)
        ListBucketResult(
          bucketName = bucketName,
          maybePrefix = params.maybePrefix,
          keyCount = contents.length,
          maxKeys = params.maxKeys,
          contents = contents
        )
      }
    }

  override def putObject(bucketName: String,
                         key: String,
                         contentSource: Source[ByteString, _]): Future[ObjectMeta] =
    fileStore.get(bucketName) match {
      case None => Future.failed(NoSuchBucketException(bucketName))
      case Some(bucketMetadata) =>
        val maybeVersioning = bucketMetadata.maybeBucketVersioning.map(_.bucketVersioning)
        saveObject(fileStream, key, bucketMetadata.path, maybeVersioning, contentSource)
          .map {
            response =>
              bucketMetadata.putObject(key, response)
              response
          }
    }

  override def getObject(bucketName: String, key: String, maybeVersionId: Option[String] = None,
                         maybeRange: Option[ByteRange] = None): Future[GetObjectResponse] =
    fileStore.get(bucketName) match {
      case None => Future.failed(NoSuchBucketException(bucketName))
      case Some(bucketMetadata) =>
        val maybeObj = bucketMetadata.getObject(key)
        if (maybeObj.isEmpty) Future.failed(NoSuchKeyException(bucketName, key))
        else {
          val meta = maybeObj.get
          getObjectPath(bucketName, key, bucketMetadata.path, meta.path, maybeVersionId) match {
            case Failure(ex) => Future.failed(ex)
            case Success(_) =>
              val sourceTuple = fileStream.downloadFile(meta.path, maybeRange = maybeRange)
              Future.successful(GetObjectResponse(bucketName, key, meta.result.etag, meta.result.contentMd5,
                sourceTuple._1.capacity, sourceTuple._2, meta.result.maybeVersionId))
          }
        }
    }

  def deleteObject(bucketName: String, key: String,
                   maybeVersionId: Option[String] = None): Future[DeleteObjectResponse.type] =
    fileStore.get(bucketName) match {
      case None => Future.failed(NoSuchBucketException(bucketName))
      case Some(bucketMetadata) =>
        val maybeObj = bucketMetadata.getObject(key)
        if (maybeObj.isEmpty) Future.failed(NoSuchKeyException(bucketName, key))
        else {
          getObjectPath(bucketName, key, bucketMetadata.path, maybeObj.get.path, maybeVersionId) match {
            case Failure(ex) => Future.failed(ex)
            case Success(objectPath) =>
              fileStore.removeObject(bucketName, key)
              fileStore.clean(objectPath.getParent)
              Future.successful(DeleteObjectResponse)
          }
        }
    }

  override def initiateMultipartUpload(bucketName: String, key: String): Future[InitiateMultipartUploadResult] =
    fileStore.get(bucketName) match {
      case None => Future.failed(NoSuchBucketException(bucketName))
      case Some(_) =>
        Future.successful {
          val uploadId = toBase16FromRandomUUID
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
              digestInfo =>
                if (Files.notExists(filePath)) Future.failed(new RuntimeException("unable to save file"))
                else {
                  bucketMetadata.addPart(uploadId, PartInfo(partNumber, digestInfo.etag))
                  val response = ObjectMeta(filePath, createPutObjectResult(key, digestInfo.etag, digestInfo.md5,
                    digestInfo.length))
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
              val (maybeVersionId, filePath) = getDestinationPathWithVersionId(key, bucketMetadata.path,
                bucketMetadata.maybeBucketVersioning.map(_.bucketVersioning))
              val result = createCompleteMultipartUploadResult(bucketName, key, parts, maybeVersionId)
              fileStream.mergeFiles(filePath, tuples.map(_._2))
                .flatMap {
                  digestInfo =>
                    if (Files.notExists(filePath)) Future.failed(new RuntimeException("unable to save file"))
                    else {
                      log.debug("etag={}, contentMD5={}, etag={}", digestInfo.etag, digestInfo.md5, result.eTag)
                      val contentLength = digestInfo.length
                      val response = ObjectMeta(filePath,
                        createPutObjectResult(key, result.eTag, digestInfo.md5, contentLength, maybeVersionId))
                      bucketMetadata.putObject(key, response)
                      Future.successful(result.copy(contentLength = contentLength))
                    }
                }
            }
          } else Future.failed(InvalidPartOrderException(bucketName, key))
        }
    }
  }


  override def copyObject(bucketName: String,
                          key: String,
                          sourceBucketName: String,
                          sourceKey: String,
                          maybeSourceVersionId: Option[String]): Future[(ObjectMeta, CopyObjectResult)] = {
    val maybeSourceBucketMetadata = fileStore.get(sourceBucketName)
    val maybeDestBucketMetaData = fileStore.get(bucketName)
    (maybeSourceBucketMetadata, maybeDestBucketMetaData) match {
      case (None, _) => Future.failed(NoSuchBucketException(sourceBucketName))
      case (_, None) => Future.failed(NoSuchBucketException(bucketName))
      case (Some(sourceBucketMetadata), Some(destBucketMetadata)) =>
        val maybeSourceObject = sourceBucketMetadata.getObject(sourceKey)
        if (maybeSourceObject.isEmpty) Future.failed(NoSuchKeyException(bucketName, key))
        else {
          val sourceObject = maybeSourceObject.get
          getObjectPath(sourceBucketName, sourceKey, sourceBucketMetadata.path, sourceObject.path,
            maybeSourceVersionId) match {
            case Failure(ex) => Future.failed(ex)
            case Success(_) =>
              val (maybeVersionId, filePath) = getDestinationPathWithVersionId(key, destBucketMetadata.path,
                destBucketMetadata.maybeBucketVersioning.map(_.bucketVersioning))
              fileStream.copyPart(sourceObject.path, filePath)
                .map {
                  digestInfo =>
                    val response = ObjectMeta(filePath, createPutObjectResult(key, digestInfo.etag, digestInfo.md5,
                      digestInfo.length,
                      maybeVersionId))
                    destBucketMetadata.putObject(key, response)
                    (response, CopyObjectResult(digestInfo.etag))
                }
          }
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
        val maybeObj = bucketMetadata.getObject(sourceKey)
        if (maybeObj.isEmpty) Future.failed(NoSuchKeyException(bucketName, key))
        else {
          val meta = maybeObj.get
          getObjectPath(sourceBucketName, sourceKey, bucketMetadata.path, meta.path,
            maybeSourceVersionId) match {
            case Failure(ex) => Future.failed(ex)
            case Success(_) =>
              val uploadPath = fileStore.uploadsDir -> (bucketName, key, uploadId)
              if (Files.notExists(uploadPath)) Future.failed(NoSuchUploadException(bucketName, key))
              else {
                val destinationPath = uploadPath -> partNumber.toString
                Files.createDirectories(destinationPath.getParent)
                fileStream.copyPart(meta.path, destinationPath, maybeSourceRange)
                  .map {
                    digestInfo =>
                      log.debug("Copy part # {} for {} with etag {}", partNumber, key, digestInfo.etag)
                      CopyPartResult(digestInfo.etag)
                  }
              }
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
  def apply(fileStore: FileStore, log: LoggingAdapter)(implicit mat: Materializer): FileRepository =
    new FileRepository(fileStore, FileStream(), log)
}
