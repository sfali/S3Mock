package com.loyalty.testing.s3.streams

import java.nio.file.{Files, Path}

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.IOResult
import akka.stream.scaladsl.{Keep, Sink}
import akka.testkit.TestKit
import akka.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.must.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.jdk.CollectionConverters._

class RangeDownloadSourceSpec
  extends TestKit(ActorSystem("test"))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with ScalaFutures {

  import RangeDownloadSourceSpec._

  private val log: LoggingAdapter = system.log
  private val fileStream = FileStream()
  private var path: Path = _

  override protected def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    path = createFile(20)
    log.info("Path created: {}", path)
    Files.exists(path) mustBe true
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    Files.delete(path)
    Files.exists(path) mustBe false
  }

  it should "create file of specified lines" in {
    Files.size(path) mustEqual (line.length + System.lineSeparator().length) * 20
  }

  it should "download entire file when range is not provided with chunk size less than file size" in {
    val source = fileStream.downloadFile(path, 43)._2
    val eventualResult = source.toMat(Sink.seq)(Keep.both).run()
    whenReady(eventualResult._1)(validateIOResult(1000))
    whenReady(eventualResult._2)(validateString(expectedString(20)))
  }

  it should "download entire file when range is not provided with chunk size greater than file size" in {
    val source = fileStream.downloadFile(path)._2
    val eventualResult = source.toMat(Sink.seq)(Keep.both).run()
    whenReady(eventualResult._1)(validateIOResult(1000))
    whenReady(eventualResult._2)(validateString(expectedString(20)))
  }

  it should "download slice of bytes of a file with chunk size greater than number of bytes" in {
    val source = fileStream.downloadFile(path, maybeRange = Some(ByteRange(0, 48)))._2
    val eventualResult = source.toMat(Sink.seq)(Keep.both).run()
    whenReady(eventualResult._1)(validateIOResult(48))
    whenReady(eventualResult._2)(validateString(line))
  }

  it should "download slice of bytes of a file with chunk size less than number of bytes" in {
    val source = fileStream.downloadFile(path, 7, Some(ByteRange(0, 48)))._2
    val eventualResult = source.toMat(Sink.seq)(Keep.both).run()
    whenReady(eventualResult._1)(validateIOResult(48))
    whenReady(eventualResult._2)(validateString(line))
  }

  it should "download offset of bytes of a file with chunk size greater than number of bytes" in {
    val source = fileStream.downloadFile(path, maybeRange = Some(ByteRange.fromOffset(900)))._2
    val eventualResult = source.toMat(Sink.seq)(Keep.both).run()
    whenReady(eventualResult._1)(validateIOResult(100))
    whenReady(eventualResult._2)(validateString(expectedString(2)))
  }

  it should "download offset of bytes of a file with chunk size less than number of bytes" in {
    val source = fileStream.downloadFile(path, 14, Some(ByteRange.fromOffset(900)))._2
    val eventualResult = source.toMat(Sink.seq)(Keep.both).run()
    whenReady(eventualResult._1)(validateIOResult(100))
    whenReady(eventualResult._2)(validateString(expectedString(2)))
  }

  it should "download suffix of bytes of a file with chunk size greater than number of bytes" in {
    val source = fileStream.downloadFile(path, maybeRange = Some(ByteRange.Suffix(50)))._2
    val eventualResult = source.toMat(Sink.seq)(Keep.both).run()
    whenReady(eventualResult._1)(validateIOResult(50))
    whenReady(eventualResult._2)(validateString(expectedString(1)))
  }

  it should "download suffix of bytes of a file with chunk size less than number of bytes" in {
    val source = fileStream.downloadFile(path, 14, Some(ByteRange.Suffix(50)))._2
    val eventualResult = source.toMat(Sink.seq)(Keep.both).run()
    whenReady(eventualResult._1)(validateIOResult(50))
    whenReady(eventualResult._2)(validateString(expectedString(1)))
  }

  private def validateIOResult(count: Long)(result: IOResult) = {
    result.count mustEqual count
    // result.status mustEqual Success(Done)
  }

  private def validateString(stringToCompare: String)(seq: Seq[ByteString]) = {
    val result = seq.fold(ByteString(""))(_ ++ _).utf8String
    result mustEqual stringToCompare
  }
}

object RangeDownloadSourceSpec {
  private val line = "A quick brown fox jumps over the silly lazy dog."

  private def loadData(numOfLines: Int) = List.fill(numOfLines)(line)

  private def createFile(numOfLines: Int, prefix: String = "sample-", suffix: String = ".txt"): Path = {
    val path = Files.createTempFile(prefix, suffix)
    Files.write(path, loadData(numOfLines).asJava)
  }

  private def expectedString(numOfLines: Int): String =
    loadData(numOfLines).mkString(System.lineSeparator()) + System.lineSeparator()
}
