package com.loyalty.testing.s3.repositories

import java.nio.file.Path

import com.loyalty.testing.s3.notification.Notification
import com.loyalty.testing.s3.request.{UploadPart, VersioningConfiguration}
import com.loyalty.testing.s3.response.ObjectMeta

import scala.collection.mutable


class BucketMetadata(val bucketName: String, val path: Path) {

  private var _location: String = _
  private var _maybeBucketVersioning: Option[VersioningConfiguration] = None
  private var _notifications: List[Notification] = Nil
  private val objectMetaMap: mutable.Map[String, ObjectMeta] = mutable.Map.empty
  private val multiPartUploads = mutable.Map[String, List[UploadPart]]()

  def location: String = _location

  def location_=(location: String): Unit = _location = location

  def maybeBucketVersioning: Option[VersioningConfiguration] = _maybeBucketVersioning

  def maybeBucketVersioning_=(value: VersioningConfiguration): Unit = _maybeBucketVersioning = Option(value)

  def notifications: List[Notification] = _notifications

  def notifications_=(ls: List[Notification]): Unit = _notifications = ls

  def putObject(key: String, metadata: ObjectMeta): Unit = objectMetaMap += (convertKey(key) -> metadata)

  def getObject(key: String): Option[ObjectMeta] = objectMetaMap.get(convertKey(key))

  def removeMetadata(key: String): Unit = objectMetaMap -= convertKey(key)

  def addPart(uploadId: String, part: UploadPart): Unit = {
    val parts = multiPartUploads.getOrElse(uploadId, List())
    multiPartUploads += (uploadId -> (parts :+ part))
  }

  def getParts(uploadId: String): List[UploadPart] = multiPartUploads.getOrElse(uploadId, List())

  def removeUpload(uploadId: String): Unit = multiPartUploads -= uploadId

  override def toString: String = {
    val keys = objectMetaMap.keys.mkString(", ")
    s"""
       |BucketName: $bucketName
       |Path: $path
       |Keys: $keys
     """.stripMargin
  }

  private def convertKey(key: String) = if (key.startsWith("/")) key else s"/$key"
}

object BucketMetadata {
  def apply(bucketName: String, path: Path): BucketMetadata = new BucketMetadata(bucketName, path)
}


