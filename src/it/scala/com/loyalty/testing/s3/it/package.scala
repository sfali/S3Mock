package com.loyalty.testing.s3

import java.nio.file.{Path, Paths}

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.loyalty.testing.s3.streams.{DigestCalculator, DigestInfo}
import com.loyalty.testing.s3.utils.StaticDateTimeProvider

import scala.concurrent.Future

package object it {

  implicit val dateTimeProvider: StaticDateTimeProvider = StaticDateTimeProvider()

  private val userDir: String = System.getProperty("user.dir")

  val rootPath: Path = Paths.get(userDir, "target", ".s3mock")

  case class ObjectInfo(bucketName: String,
                        key: String,
                        eTag: String,
                        contentMd5: String,
                        contentLength: Long,
                        versionId: Option[String] = None)

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

  val resourcePath: Path = Paths.get("src", "it", "resources")
  val defaultBucketName: String = "non-versioned-bucket"
  val versionedBucketName: String = "versioned-bucket"
  val nonExistentBucketName: String = "dummy"
  val etagDigest: String = "6b4bb2a848f1fac797e320d7b9030f3e"
  // private val md5Digest = "a0uyqEjx+seX4yDXuQMPPg=="
  val etagDigest1: String = "84043a46fafcdc5451db399625915436"
  // private val md5Digest1 = "hAQ6Rvr83FRR2zmWJZFUNg=="
}
