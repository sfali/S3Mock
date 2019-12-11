package com.loyalty.testing.s3

import java.nio.file.{Path, Paths}

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.loyalty.testing.s3.response.CopyObjectResult
import com.loyalty.testing.s3.streams.{DigestCalculator, DigestInfo}
import com.loyalty.testing.s3.utils.StaticDateTimeProvider

import scala.concurrent.Future

package object test {

  implicit val dateTimeProvider: StaticDateTimeProvider = StaticDateTimeProvider()

  private val bucketIdentifier: String => (String, String) = bucketName => (bucketName, bucketName.toUUID.toString)

  def createContentSource(start: Int, totalSize: Int): Source[ByteString, NotUsed] =
    Source
      .repeat("A quick brown fox jumps over the silly lazy dog.")
      .take(totalSize)
      .zipWithIndex
      .map {
        case (s, index) => s"${index + start}. $s\r\n"
      }
      .map(ByteString(_))

  def calculateDigest(start: Int, totalSize: Int)(implicit mat: Materializer): Future[DigestInfo] =
    createContentSource(start, totalSize)
      .via(DigestCalculator()).runWith(Sink.head)

  val userDir: String = System.getProperty("user.dir")
  val rootPath: Path = Paths.get(userDir, "target", ".s3mock")
  val resourcePath: Path = Paths.get("src", "test", "resources")
  val (defaultBucketName, defaultBucketNameUUID) = bucketIdentifier("non-versioned-bucket")
  val (versionedBucketName, versionedBucketNameUUID) = bucketIdentifier("versioned-bucket")
  val (bucket2, bucket2UUID) = bucketIdentifier("other-bucket-1")
  val (bucket3, bucket3UUID) = bucketIdentifier("other-bucket-2")
  val (nonExistentBucketName, nonExistentBucketUUID) = bucketIdentifier("dummy")
  val etagDigest: String = "6b4bb2a848f1fac797e320d7b9030f3e"
  val md5Digest: String = "a0uyqEjx+seX4yDXuQMPPg=="
  val etagDigest1: String = "84043a46fafcdc5451db399625915436"
  val md5Digest1: String = "hAQ6Rvr83FRR2zmWJZFUNg=="

  implicit class CopyObjectResultOps(src: CopyObjectResult) {
    def merge(target: CopyObjectResult): CopyObjectResult =
      src
        .copy(
          maybeVersionId = target.maybeVersionId,
          maybeSourceVersionId = target.maybeSourceVersionId,
          lastModifiedDate = target.lastModifiedDate
        )
  }

}
