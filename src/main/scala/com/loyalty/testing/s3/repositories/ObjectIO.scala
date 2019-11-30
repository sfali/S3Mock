package com.loyalty.testing.s3.repositories

import java.nio.file.{Files, Path}
import java.time.OffsetDateTime
import java.util.UUID
import java.util.stream.Collectors

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.IOResult
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.repositories.model.{Bucket, ObjectKey}
import com.loyalty.testing.s3.request.BucketVersioning
import com.loyalty.testing.s3.streams.FileStream

import scala.concurrent.Future

class ObjectIO(root: Path, fileStream: FileStream)
              (implicit system: ActorSystem[Nothing]) {

  import system.executionContext

  private val workDir: Path = root.toAbsolutePath
  private val dataDir: Path = workDir + "data"
  // private val uploadsDir: Path = workDir + "uploads"

  def saveObject(bucket: Bucket,
                 key: String,
                 keyId: UUID,
                 versionIndex: Int,
                 contentSource: Source[ByteString, _]): Future[ObjectKey] = {
    val versionId = versionIndex.toVersionId
    val objectPath = getObjectPath(bucket.bucketName, key, bucket.version, versionId)
    fileStream.saveContent(contentSource, objectPath)
      .flatMap {
        case (etag, contentMD5) =>
          if (Files.notExists(objectPath)) Future.failed(new RuntimeException("unable to save file"))
          else {
            val objectKey = ObjectKey(
              id = keyId,
              bucketName = bucket.bucketName,
              key = key,
              index = versionIndex,
              version = bucket.version,
              versionId = versionId,
              eTag = etag,
              contentMd5 = contentMD5,
              contentLength = Files.size(objectPath),
              lastModifiedTime = OffsetDateTime.now()
            )
            Future.successful(objectKey)
          }
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

  private def getObjectPath(bucketName: String,
                            key: String,
                            bucketVersioning: BucketVersioning,
                            versionId: String) = {
    val objectParentPath = dataDir -> (bucketName, key, toBase16(bucketVersioning.entryName), versionId)
    Files.createDirectories(objectParentPath)
    objectParentPath -> ContentFileName
  }
}

object ObjectIO {
  def apply(root: Path, fileStream: FileStream)
           (implicit system: ActorSystem[Nothing]): ObjectIO = new ObjectIO(root, fileStream)
}
