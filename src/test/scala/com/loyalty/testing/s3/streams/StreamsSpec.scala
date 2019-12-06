package com.loyalty.testing.s3.streams

import java.nio.file.{Files, Paths}

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.ByteRange
import akka.http.scaladsl.model.{HttpEntity, HttpRequest}
import akka.stream.scaladsl.{FileIO, Keep, Sink}
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import scala.jdk.CollectionConverters._

class StreamsSpec
  extends TestKit(ActorSystem("test"))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  import com.loyalty.testing.s3._

  private implicit val defaultPatience: PatienceConfig = PatienceConfig(timeout = Span(15, Seconds),
    interval = Span(500, Millis))

  private val fileStream = FileStream()
  private val basePath = "src/test/resources/"
  private val srcPath = Paths.get(basePath, "sample.txt").toAbsolutePath
  private val etagDigest = "6b4bb2a848f1fac797e320d7b9030f3e"
  private val md5Digest = "a0uyqEjx+seX4yDXuQMPPg=="

  override protected def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }

  it should "calculate the correct digest" in {
    val eventualDigest =
      FileIO
        .fromPath(srcPath)
        .via(DigestCalculator())
        .runWith(Sink.head)
    val digestInfo = eventualDigest.futureValue
    etagDigest mustEqual digestInfo.etag
    md5Digest mustEqual digestInfo.md5
    digestInfo.length mustEqual Files.size(srcPath)
  }

  it should "save bytes and calculate digest" in {
    // val srcContentLength = Files.size(srcPath)

    val destinationPath = Files.createTempFile("test", ".txt")
    val (_, eventualDigest) =
      FileIO
        .fromPath(srcPath)
        .via(fileStream.saveAndCalculateDigest(destinationPath))
        .toMat(Sink.head)(Keep.both)
        .run()

    val digestInfo = eventualDigest.futureValue
    etagDigest mustEqual digestInfo.etag
    md5Digest mustEqual digestInfo.md5
    digestInfo.length mustEqual Files.size(srcPath)

    /*val ioResult: IOResult = eventualIoResult.futureValue
    ioResult.status match {
      case Success(_) =>
        val destContentLength = Files.size(destinationPath)
        val ioCount = ioResult.count
        srcContentLength mustEqual destContentLength
        srcContentLength mustEqual ioCount
        Files.deleteIfExists(destinationPath)
      case Failure(ex) =>
        Files.deleteIfExists(destinationPath)
        fail(ex.getMessage)
    }*/
  }

  it should "save HttpRequest entity to given path & calculate digest" in {
    val destinationPath = Files.createTempFile("test", ".txt")
    val entity = HttpEntity(Files.readAllBytes(srcPath))
    val request = HttpRequest(entity = entity)
    val digestInfo = fileStream.saveContent(request.entity.dataBytes, destinationPath).futureValue
    Files.exists(destinationPath) mustBe true
    Files.deleteIfExists(destinationPath)
    etagDigest mustEqual digestInfo.etag
    md5Digest mustEqual digestInfo.md5
    digestInfo.length mustEqual Files.size(srcPath)
  }

  it should "merge files and calculate digest" in {
    val files = Files.list(Paths.get(basePath, "sub-files")).iterator().asScala.toList
    val destinationPath = Files.createTempFile("test", ".txt")
    val digestInfo = fileStream.mergeFiles(destinationPath, files).futureValue
    Files.exists(destinationPath) mustBe true
    Files.deleteIfExists(destinationPath)
    etagDigest mustEqual digestInfo.etag
    md5Digest mustEqual digestInfo.md5
    digestInfo.length mustEqual Files.size(srcPath)
  }

  it should "copy entire file to destination path when no range is provided" in {
    val sourcePath = Paths.get("src", "test", "resources", "sample.txt")
    val destinationPath = Files.createTempFile("test", ".txt")
    val digestInfo = fileStream.copyPart(sourcePath, destinationPath).futureValue
    Files.size(sourcePath) must equal(Files.size(destinationPath))
    etagDigest mustEqual digestInfo.etag
    md5Digest mustEqual digestInfo.md5
    digestInfo.length mustEqual Files.size(srcPath)
    Files.deleteIfExists(destinationPath)
  }

  it should "copy range of bytes from source to destination when range is provided" in {
    val sourcePath = Paths.get("src", "test", "resources", "sample.txt")
    val destinationPath = Files.createTempFile("test", ".txt")
    val digestInfo = fileStream.copyPart(sourcePath, destinationPath, Some(ByteRange(265, 318))).futureValue
    digestInfo.etag mustEqual toBase16("6. A quick brown fox jumps over the silly lazy dog.\r\n")
    digestInfo.md5 mustEqual toBase64("6. A quick brown fox jumps over the silly lazy dog.\r\n")
    digestInfo.length mustEqual 53
    Files.deleteIfExists(destinationPath)
  }

}
