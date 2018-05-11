package com.loyalty.testing.s3.streams

import java.nio.file.{Files, Paths}

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.ByteRange
import akka.http.scaladsl.model.{HttpEntity, HttpRequest}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Keep, Sink}
import akka.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, MustMatchers}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

class StreamsSpec
  extends TestKit(ActorSystem("test"))
    with FlatSpecLike
    with MustMatchers
    with BeforeAndAfterAll
    with ScalaFutures {

  import com.loyalty.testing.s3._

  private implicit val mat: ActorMaterializer = ActorMaterializer()
  private val fileStream = FileStream()
  private val basePath = "src/test/resources/"
  private val srcPath = Paths.get(basePath, "sample.txt").toAbsolutePath
  private val expectedDigest = "37099E6F8B99C52CD81DF0041543E5B0"

  override protected def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }

  it should "calculate the correct diget" in {
    val eventualDigest =
      FileIO
        .fromPath(srcPath)
        .via(DigestCalculator())
        .runWith(Sink.head)

    whenReady(eventualDigest) {
      digest => expectedDigest must equal(digest)
    }
  }

  it should "save bytes and calculate digest" in {
    val srcContentLength = Files.size(srcPath)

    val destinationPath = Files.createTempFile("test", ".txt")
    val (eventualIoResult, eventualDigest) =
      FileIO
        .fromPath(srcPath)
        .via(fileStream.saveAndCalculateDigest(destinationPath))
        .toMat(Sink.head)(Keep.both)
        .run()

    whenReady(eventualDigest) {
      digest => expectedDigest must equal(digest)
    }

    whenReady(eventualIoResult) {
      ioResult =>
        ioResult.status match {
          case Success(_) =>
            val destContentLength = Files.size(destinationPath)
            val ioCount = ioResult.count
            srcContentLength must equal(destContentLength)
            srcContentLength must equal(ioCount)
            Files.deleteIfExists(destinationPath)
          case Failure(ex) =>
            Files.deleteIfExists(destinationPath)
            fail(ex.getMessage)
        }
    }
  }

  it should "save HttpRequest entity to given path & calculate digest" in {
    val destinationPath = Files.createTempFile("test", ".txt")
    val entity = HttpEntity(Files.readAllBytes(srcPath))
    val request = HttpRequest(entity = entity)

    val eventualDigest = fileStream.saveContent(request.entity.dataBytes, destinationPath)

    whenReady(eventualDigest) {
      digest =>
        Files.exists(destinationPath) mustBe true
        Files.deleteIfExists(destinationPath)
        expectedDigest must equal(digest)
    }
  }

  it should "merge files and calculate digest" in {
    val files = Files.list(Paths.get(basePath, "sub-files")).iterator().asScala.toList

    val destinationPath = Files.createTempFile("test", ".txt")
    val eventualDigest = fileStream.mergeFiles(destinationPath, files)

    whenReady(eventualDigest) {
      digest =>
        Files.exists(destinationPath) mustBe true
        Files.deleteIfExists(destinationPath)
        expectedDigest must equal(digest)
    }
  }

  it should "download all bytes" in {
    val path = Paths.get("src", "test", "resources", "sample.txt")
    whenReady(fileStream.downloadFile(path)) {
      bs => expectedDigest must equal(md5Hex(bs.toArray))
    }
  }

  it should "download range of bytes" in {
    val path = Paths.get("src", "test", "resources", "sample.txt")
    whenReady(fileStream.downloadFile(path, Some(ByteRange(300, 349)))) {
      bs =>
        bs.utf8String must equal("A quick brown fox jumps over the silly lazy dog.\r\n")
    }
  }

  it should "copy entire file to destination path when no range is provided" in {
    val sourcePath = Paths.get("src", "test", "resources", "sample.txt")
    val destinationPath = Files.createTempFile("test", ".txt")

    whenReady(fileStream.copyPart(sourcePath, destinationPath)) {
      eTag =>
        Files.size(sourcePath) must equal(Files.size(destinationPath))
        eTag must equal(expectedDigest)
        Files.deleteIfExists(destinationPath)

    }
  }

  it should "copy range of bytes from source to destination when range is provided" in {
    val sourcePath = Paths.get("src", "test", "resources", "sample.txt")
    val destinationPath = Files.createTempFile("test", ".txt")

    whenReady(fileStream.copyPart(sourcePath, destinationPath, Some(ByteRange(300, 349)))){
      eTag =>
        eTag must equal(md5Hex("A quick brown fox jumps over the silly lazy dog.\r\n"))
        Files.deleteIfExists(destinationPath)
    }
  }

}
