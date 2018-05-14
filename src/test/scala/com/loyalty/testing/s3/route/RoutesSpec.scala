package com.loyalty.testing.s3.route

import java.nio.file.{Path, Paths}

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpHeader}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.testkit.TestProbe
import com.amazonaws.services.s3.Headers
import com.loyalty.testing.s3.Settings
import com.loyalty.testing.s3.repositories.{FileRepository, FileStore}
import com.loyalty.testing.s3.routes.S3Routes
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, MustMatchers}

class RoutesSpec
  extends FlatSpecLike
    with MustMatchers
    with ScalatestRouteTest
    with S3Routes
    with BeforeAndAfterAll
    with ScalaFutures {

  import ContentTypes._
  import Headers._

  private val dataPath: Path = Paths.get(System.getProperty("user.dir"), ".s3mock")

  override protected implicit val mat: ActorMaterializer = ActorMaterializer()
  override protected val log: LoggingAdapter = system.log
  override protected val repository: FileRepository = FileRepository(FileStore(dataPath), log)
  private implicit val settings: Settings = Settings()
  override protected val notificationRouter: ActorRef = TestProbe().ref

  override protected def afterAll(): Unit = {
    super.afterAll()
    repository.clean()
  }

  it should "create bucket" in {
    Put("/test-bucket") ~> s3Routes ~> {
      check {
        headers.head mustBe Location("/test-bucket")
        status mustBe OK
      }
    }
  }

  "Attempt to create bucket, which is already exists " should " result in 404(BadRequest)" in {
    Put("/test-bucket") ~> s3Routes ~> {
      check {
        status mustBe BadRequest
        val eventualResponse = responseEntity.dataBytes.map(_.utf8String).runWith(Sink.head)
        // TODO:
        whenReady(eventualResponse) {
          response => println(response)
        }
      }
    }
  }

  it should "create bucket with region provided" in {
    val xml =
      """
        |<CreateBucketConfiguration xmlns="http:/.amazonaws.com/doc/2006-03-01/">
        | <LocationConstraint>us-west-1</LocationConstraint>
        |</CreateBucketConfiguration>
      """.stripMargin
    val entity = HttpEntity(`text/xml(UTF-8)`, xml)
    Put(s"/test-bucket-2", entity) ~> s3Routes ~> {
      check {
        headers.head mustBe Location("/test-bucket-2")
        status mustBe OK
      }
    }
  }

  it should "set versioning on the bucket" in {
    val xml =
      """
        |<VersioningConfiguration xmlns="http:/.amazonaws.com/doc/2006-03-01/">
        |  <Status>Enabled</Status>
        |</VersioningConfiguration>
      """.stripMargin
    val entity = HttpEntity(`text/xml(UTF-8)`, xml)
    Put("/test-bucket-2?versioning", entity) ~> s3Routes ~> {
      check {
        headers.head mustBe Location("/test-bucket-2")
        status mustBe OK
      }
    }
  }

  it should "put object in a bucket where versioning is not enabled" in {
    val content =
      s"""
         |Hello,
         |A quick brown fox jumps over the silly lazy dog.
         |Bye.
      """.stripMargin
    val entity = HttpEntity(`text/plain(UTF-8)`, content)
    val request = Put("/test-bucket/dummy.txt", entity)
      .addHeader(RawHeader(CONTENT_LENGTH, content.length.toString))
    request ~> s3Routes ~> {
      check {
        status mustBe OK
        val maybeContentLength = getHeader(headers, CONTENT_LENGTH)
        maybeContentLength.fold(fail(s"Unable to find $CONTENT_LENGTH header.")) {
          contentLength => contentLength.value().toInt must equal(content.length)
        }
      }
    }
  }

  it should "put object in a bucket where versioning is enabled" in {
    val content =
      s"""
         |Hello,
         |A quick brown fox jumps over the silly lazy dog.
         |Bye.
      """.stripMargin
    val entity = HttpEntity(`text/plain(UTF-8)`, content)
    val request = Put("/test-bucket-2/dummy.txt", entity)
      .addHeader(RawHeader("Content-Length", content.length.toString))
    request ~> s3Routes ~> {
      check {
        val maybeVersionId = getHeader(headers, S3_VERSION_ID)
        maybeVersionId must not be empty
        status mustBe OK
      }
    }
  }

  it should "initiate multi part upload" in {
    Post("/test-bucket/file.txt?uploads") ~> s3Routes ~> {
      check {
        status mustBe OK
      }
    }
  }

  it should "upload multi part" in {
    Put("/test-bucket/file.txt?partNumber=1&uploadId=asdf") ~> s3Routes ~> {
      check {
        status mustBe OK
      }
    }
  }

  it should "Range" in {
    val rangeHeader = Range(ByteRange.suffix(20))
    Get("/test-bucket/file.txt") ~> rangeHeader ~> s3Routes ~> {
      check {
        status mustBe OK
      }
    }
  }

  import com.loyalty.testing.s3.routes.s3.`object`.directives._

  it should "copy" in {
    val sourceHeader = `x-amz-copy-source`.parse("/test/input/test.txt?versionId=ooo") .toOption.get
    val h = `x-amz-copy-source-range`.parse("bytes=50-100").toOption.get
    Put("/test-bucket/file.txt?partNumber=1&uploadId=asdfqwer") ~> sourceHeader ~> h ~> s3Routes ~> {
      check {
        status mustBe OK
      }
    }
  }

  it should "" in {
    Head("/test-bucket/file.txt") ~> s3Routes ~> {
      check {
        status mustBe OK
        headers.foreach(println)
      }
    }
  }

  /*it should "complete multi part upload" in {
    Post("/test-bucket?uploadId=asdf") ~> s3Routes ~> {
      check {
        status mustBe OK
      }
    }
  }*/

  private def getHeader(headers: Seq[HttpHeader], headerName: String): Option[HttpHeader] =
    headers.find(_.lowercaseName() == headerName.toLowerCase)

}
