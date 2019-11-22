package com.loyalty.testing.s3.route

import java.nio.file.{Path, Paths}
import java.time.OffsetDateTime

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.util.Timeout
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.SpawnBehavior
import com.loyalty.testing.s3.actor.SpawnBehavior.Command
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.response._
import com.loyalty.testing.s3.routes.{CustomMarshallers, Routes}
import com.loyalty.testing.s3.streams.FileStream
import com.loyalty.testing.s3.test._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.duration._

class RoutesSpec
  extends AnyFlatSpec
    with Matchers
    with ScalatestRouteTest
    with Routes
    with CustomMarshallers
    with BeforeAndAfterAll
    with ScalaFutures {

  import RoutesSpec._

  protected implicit val spawnSystem: ActorSystem[Command] = ActorSystem(SpawnBehavior(), "s3mock")
  protected override implicit val timeout: Timeout = Timeout(10.seconds)
  private implicit val defaultPatience: PatienceConfig = PatienceConfig(timeout = Span(15, Seconds),
    interval = Span(500, Millis))
  private val settings = Settings(spawnSystem.settings.config)
  protected override val objectIO = ObjectIO(rootPath, FileStream())
  protected override val database = NitriteDatabase(rootPath, settings.dbSettings)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    dateTimeProvider.currentOffsetDateTime = OffsetDateTime.now()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    database.close()
    clean(rootPath)
    system.terminate()
  }

  it should "create bucket in default region" in {
    Put("/test-bucket") ~> routes ~> check {
      status mustBe OK
      headers.head mustBe Location("/test-bucket")
    }
  }

  "Attempt to create bucket, which is already exists " should " result in 404(BadRequest)" in {
    Put("/test-bucket") ~> routes ~> check {
      status mustBe BadRequest
      responseAs[BucketAlreadyExistsException] mustEqual BucketAlreadyExistsException("test-bucket")
    }
  }

 /* it should "create bucket with region provided" in {
    val xml =
      """
        |<CreateBucketConfiguration xmlns="http:/.amazonaws.com/doc/2006-03-01/">
        | <LocationConstraint>us-west-1</LocationConstraint>
        |</CreateBucketConfiguration>
      """.stripMargin
    val entity = HttpEntity(`text/xml(UTF-8)`, xml)
    Put(s"/test-bucket-2", CreateBucketConfiguration("us-west-1")) ~> routes ~> check {
      headers.head mustBe Location("/test-bucket-2")
      status mustBe OK
    }
  }*/

  /*it should "set versioning on the bucket" in {
    val xml =
      """
        |<VersioningConfiguration xmlns="http:/.amazonaws.com/doc/2006-03-01/">
        |  <Status>Enabled</Status>
        |</VersioningConfiguration>
      """.stripMargin
    val entity = HttpEntity(`text/xml(UTF-8)`, xml)
    Put("/test-bucket-2?versioning", entity) ~> s3Routes ~> check {
      headers.head mustBe Location("/test-bucket-2")
      status mustBe OK
    }
  }*/

  /*ignore should "put object in a bucket where versioning is not enabled" in {
    val content =
      s"""
         |Hello,
         |A quick brown fox jumps over the silly lazy dog.
         |Bye.
      """.stripMargin
    val entity = HttpEntity(`text/plain(UTF-8)`, content)
    val request = Put("/test-bucket/dummy.txt", entity)
      .addHeader(RawHeader(CONTENT_LENGTH, content.length.toString))
    request ~> s3Routes ~> check {
      status mustBe OK
      val maybeContentLength = getHeader(headers, CONTENT_LENGTH)
      maybeContentLength.fold(fail(s"Unable to find $CONTENT_LENGTH header.")) {
        contentLength => contentLength.value().toInt must equal(content.length)
      }
    }
  }*/

  /*it should "put object in a bucket where versioning is enabled" in {
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
  }*/

  /*it should "initiate multi part upload" in {
    Post("/test-bucket/file.txt?uploads") ~> s3Routes ~> {
      check {
        status mustBe OK
      }
    }
  }*/

  /*ignore should "upload multi part" in {
    Put("/test-bucket/file.txt?partNumber=1&uploadId=asdf") ~> s3Routes ~> {
      check {
        status mustBe OK
      }
    }
  }*/

  /*ignore should "Range" in {
    val rangeHeader = Range(ByteRange.suffix(20))
    Get("/test-bucket/file.txt") ~> rangeHeader ~> s3Routes ~> {
      check {
        status mustBe OK
      }
    }
  }*/

  // import com.loyalty.testing.s3.routes.s3.`object`.directives._

  /*ignore should "copy" in {
    val sourceHeader = `x-amz-copy-source`.parse("/test/input/test.txt?versionId=ooo").toOption.get
    val h = `x-amz-copy-source-range`.parse("bytes=50-100").toOption.get
    Put("/test-bucket/file.txt?partNumber=1&uploadId=asdfqwer") ~> sourceHeader ~> h ~> s3Routes ~> {
      check {
        status mustBe OK
      }
    }
  }*/

  /*it should "" in {
    Head("/test-bucket/file.txt") ~> s3Routes ~> {
      check {
        status mustBe OK
        headers.foreach(println)
      }
    }
  }*/

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

object RoutesSpec {
  private val userDir: String = System.getProperty("user.dir")
  private val rootPath: Path = Paths.get(userDir, "target", ".s3mock")
  private val resourcePath = Paths.get("src", "test", "resources")
  private val defaultBucketName = "non-versioned-bucket"
  // private val defaultBucketNameUUID = defaultBucketName.toUUID.toString
  private val versionedBucketName = "versioned-bucket"
  // private val versionedBucketNameUUID = versionedBucketName.toUUID.toString
  private val nonExistentBucketName = "dummy"
  // private val nonExistentBucketUUID = nonExistentBucketName.toUUID.toString
}
