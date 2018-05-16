package com.loyalty.testing

import java.net.{URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import java.util.UUID

import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectResult}
import com.amazonaws.services.sns.AmazonSNSAsync
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.loyalty.testing.s3.request.UploadPart
import com.loyalty.testing.s3.response.CompleteMultipartUploadResult
import javax.xml.bind.DatatypeConverter

package object s3 {

  val defaultRegion: String = "us-east-1"

  private val md = MessageDigest.getInstance("MD5")

  def md5Hex(s: String): String = {
    md.reset()
    md.update(s.getBytes)
    DatatypeConverter.printHexBinary(md.digest())
  }

  def md5Hex(bytes: Array[Byte]): String = {
    md.reset()
    md.update(bytes)
    DatatypeConverter.printHexBinary(md.digest())
  }

  def md5Hex(path: Path): String = md5Hex(Files.readAllBytes(path))

  def md5HexFromRandomUUID: String = md5Hex(UUID.randomUUID().toString)

  implicit class StringOps(s: String) {
    def decode: String = URLDecoder.decode(s, UTF_8.toString)

    def encode: String = URLEncoder.encode(s, UTF_8.toString)
  }

  implicit class PathOps(path: Path) {

    /**
      * Append the given `other` path and create directories if not exists.
      *
      * @param other other path to append
      * @return new path
      */
    def +(other: Path): Path = {
      val result = Paths.get(path.toString, other.toString)
      createDirectories(result)
      result
    }

    def +(other: String*): Path = {
      val result = Paths.get(path.toString, other: _*)
      createDirectories(result)
      result
    }

    def ->(other: String*): Path = Paths.get(path.toString, other: _*)
  }

  def createCompleteMultipartUploadResult(bucketName: String,
                                          key: String,
                                          parts: List[UploadPart],
                                          maybeVersionId: Option[String] = None): CompleteMultipartUploadResult = {
    val hex = md5Hex(parts.map(_.eTag).mkString)
    val eTag = s"$hex-${parts.length}"

    CompleteMultipartUploadResult(bucketName, key, eTag, 0L, maybeVersionId)
  }

  def createPutObjectResult(eTag: String,
                            contentMd5: String,
                            contentLength: Long,
                            maybeVersionId: Option[String] = None): PutObjectResult = {
    val objectMetadata = new ObjectMetadata()
    objectMetadata.setContentLength(contentLength)
    objectMetadata.setContentMD5(contentMd5)
    val result = new PutObjectResult()
    result.setContentMd5(contentMd5)
    result.setETag(eTag)
    result.setMetadata(objectMetadata)
    maybeVersionId.fold(result) {
      versionId =>
        result.setVersionId(versionId)
        result
    }
  }

  trait SqsSettings {
    val sqsClient: AmazonSQSAsync
  }

  trait SnsSettings {
    val snsClient: AmazonSNSAsync
  }

  private def createDirectories(path: Path): Unit = {
    if (Files.notExists(path)) Files.createDirectories(path)
  }

}
