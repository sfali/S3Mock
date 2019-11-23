package com.loyalty.testing.s3.route

import java.nio.file.{Files, Path, Paths}
import java.time.OffsetDateTime

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.{HttpEntity, HttpHeader}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.scaladsl.{FileIO, Sink}
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
    Put(s"/$defaultBucketName") ~> routes ~> check {
      status mustBe OK
      headers.head mustBe Location(s"/$defaultBucketName")
    }
  }

  it should "send 404(BadRequest) if attempt to create bucket, which is already exists" in {
    Put(s"/$defaultBucketName") ~> routes ~> check {
      status mustBe BadRequest
      responseAs[BucketAlreadyExistsException] mustEqual BucketAlreadyExistsException(defaultBucketName)
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

  it should "put an object in the specified non-version bucket" in {
    val key = "sample.txt"
    val path = resourcePath -> key
    val contentSource = FileIO.fromPath(path)
    val entity = HttpEntity(`application/octet-stream`, contentSource)
    Put(s"/$defaultBucketName/$key", entity) ~> routes ~> check {
      status mustEqual OK
      getHeader(headers, ETAG) mustBe Some(RawHeader(ETAG, s""""$etagDigest""""))
      getHeader(headers, CONTENT_MD5) mustBe Some(RawHeader(CONTENT_MD5, s"$md5Digest"))
    }
  }

  it should "update object in non-version bucket" in {
    val key = "sample.txt"
    val path = resourcePath -> "sample1.txt"
    val contentSource = FileIO.fromPath(path)
    val entity = HttpEntity(`application/octet-stream`, contentSource)
    Put(s"/$defaultBucketName/$key", entity) ~> routes ~> check {
      status mustEqual OK
      getHeader(headers, ETAG) mustBe Some(RawHeader(ETAG, s""""$etagDigest1""""))
      getHeader(headers, CONTENT_MD5) mustBe Some(RawHeader(CONTENT_MD5, s"$md5Digest1"))
    }
  }

  it should "put a multi-path object in the specified non-version bucket" in {
    val fileName = "sample.txt"
    val key = s"input/$fileName"
    val path = resourcePath -> fileName
    val contentSource = FileIO.fromPath(path)
    val entity = HttpEntity(`application/octet-stream`, contentSource)
    Put(s"/$defaultBucketName/$key", entity) ~> routes ~> check {
      status mustEqual OK
      getHeader(headers, ETAG) mustBe Some(RawHeader(ETAG, s""""$etagDigest""""))
      getHeader(headers, CONTENT_MD5) mustBe Some(RawHeader(CONTENT_MD5, s"$md5Digest"))
    }
  }

  it should "result in 404(BadRequest) if attempt to put object in non-existing bucket" in {
    val fileName = "sample.txt"
    val key = s"input/$fileName"
    val path = resourcePath -> fileName
    val contentSource = FileIO.fromPath(path)
    val entity = HttpEntity(`application/octet-stream`, contentSource)
    Put(s"/$nonExistentBucketName/$key", entity) ~> routes ~> check {
      status mustEqual NotFound
      responseAs[NoSuchBucketException] mustEqual NoSuchBucketException(nonExistentBucketName)
    }
  }

  it should "get entire object when range is not specified" in {
    val key = "sample.txt"
    val path = resourcePath -> "sample1.txt"
    val expectedContent = FileIO.fromPath(path).map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    Get(s"/$defaultBucketName/$key") ~> routes ~> check {
      status mustEqual OK
      getHeader(headers, ETAG) mustBe Some(RawHeader(ETAG, s""""$etagDigest1""""))
      getHeader(headers, CONTENT_MD5) mustBe Some(RawHeader(CONTENT_MD5, s"$md5Digest1"))
      response.entity.contentLengthOption mustBe Some(Files.size(path))
      val actualContent = response.entity.dataBytes.map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
      expectedContent mustEqual actualContent
    }
  }

  it should "get object with range between two positions from the start of file" in {
    val key = "sample.txt"
    val expectedContent = "1. A quick brown fox jumps over the silly lazy dog.\r\n"
    val rangeHeader = Range(ByteRange(0, 53))
    Get(s"/$defaultBucketName/$key").withHeaders(rangeHeader :: Nil) ~> routes ~> check {
      status mustEqual OK
      getHeader(headers, ETAG) mustBe Some(RawHeader(ETAG, s""""$etagDigest1""""))
      getHeader(headers, CONTENT_MD5) mustBe Some(RawHeader(CONTENT_MD5, s"$md5Digest1"))
      response.entity.contentLengthOption mustBe Some(53)
      val actualContent = response.entity.dataBytes.map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
      expectedContent mustEqual actualContent
    }
  }

  /*it should "put an object in the specified bucket with bucket versioning on" in {
    val key = "sample.txt"
    val path = resourcePath -> key
    val contentSource = FileIO.fromPath(path)
    val entity = HttpEntity(`application/octet-stream`, contentSource)
    Put(s"/$versionedBucketName/$key", entity) ~> routes ~> check {
      status mustEqual OK
      getHeader(headers, ETAG) mustBe Some(RawHeader(ETAG, s""""$etagDigest""""))
      getHeader(headers, CONTENT_MD5) mustBe Some(RawHeader(CONTENT_MD5, s"$md5Digest"))
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
  //private val versionedBucketName = "versioned-bucket"
  private val nonExistentBucketName = "dummy"
  private val etagDigest = "6b4bb2a848f1fac797e320d7b9030f3e"
  private val md5Digest = "a0uyqEjx+seX4yDXuQMPPg=="
  private val etagDigest1 = "84043a46fafcdc5451db399625915436"
  private val md5Digest1 = "hAQ6Rvr83FRR2zmWJZFUNg=="
}
