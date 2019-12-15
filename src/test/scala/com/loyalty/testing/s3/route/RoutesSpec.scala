package com.loyalty.testing.s3.route

import java.nio.file.Files
import java.time.{Instant, OffsetDateTime}

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
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

  protected implicit val spawnSystem: ActorSystem[Command] = ActorSystem(SpawnBehavior(), "s3mock")
  protected override implicit val timeout: Timeout = Timeout(10.seconds)
  private implicit val defaultPatience: PatienceConfig = PatienceConfig(timeout = Span(15, Seconds),
    interval = Span(500, Millis))
  private val settings = AppSettings(spawnSystem.settings.config)
  protected override val objectIO: ObjectIO = ObjectIO(rootPath, FileStream())
  protected override val database: NitriteDatabase = NitriteDatabase(rootPath, settings.dbSettings)
  private val xmlContentType = ContentType(MediaTypes.`application/xml`, HttpCharsets.`UTF-8`)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    dateTimeProvider.currentOffsetDateTime = OffsetDateTime.now()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    database.close()
    Files.delete(rootPath -> settings.dbSettings.fileName)
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

  it should "create bucket with region provided" in {
    val xml =
      """
        |<CreateBucketConfiguration xmlns="http:/.amazonaws.com/doc/2006-03-01/">
        |<LocationConstraint>us-west-1</LocationConstraint>
        |</CreateBucketConfiguration>
       """.stripMargin.replaceAll(System.lineSeparator(), "")
    val entity = HttpEntity(xmlContentType, xml)
    Put(s"/$versionedBucketName", entity) ~> routes ~> check {
      headers.head mustBe Location(s"/$versionedBucketName")
      status mustBe OK
    }
  }

  it should "set versioning on the bucket" in {
    val xml =
      """
        |<VersioningConfiguration xmlns="http:/.amazonaws.com/doc/2006-03-01/">
        |<Status>Enabled</Status>
        |</VersioningConfiguration>
      """.stripMargin.replaceAll(System.lineSeparator(), "")
    val entity = HttpEntity(xmlContentType, xml)
    Put(s"/$versionedBucketName?versioning", entity) ~> routes ~> check {
      headers.head mustBe Location(s"/$versionedBucketName")
      status mustBe OK
    }
  }

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

  it should "result in 404(NotFound) if attempt to put object in non-existing bucket" in {
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
      expectedContent mustEqual getContent(response)
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
      expectedContent mustEqual getContent(response)
    }
  }

  it should "put an object in the specified bucket with bucket versioning on" in {
    val key = "sample.txt"
    val path = resourcePath -> key
    val contentSource = FileIO.fromPath(path)
    val entity = HttpEntity(`application/octet-stream`, contentSource)
    Put(s"/$versionedBucketName/$key", entity) ~> routes ~> check {
      status mustEqual OK
      getHeader(headers, ETAG) mustBe Some(RawHeader(ETAG, s""""$etagDigest""""))
      getHeader(headers, CONTENT_MD5) mustBe Some(RawHeader(CONTENT_MD5, s"$md5Digest"))
      getHeader(headers, VersionIdHeader) mustBe Some(RawHeader(VersionIdHeader, 1.toVersionId))
    }
  }

  it should "update object in the specified bucket with bucket versioning on" in {
    val key = "sample.txt"
    val path = resourcePath -> "sample1.txt"
    val contentSource = FileIO.fromPath(path)
    val entity = HttpEntity(`application/octet-stream`, contentSource)
    Put(s"/$versionedBucketName/$key", entity) ~> routes ~> check {
      status mustEqual OK
      getHeader(headers, ETAG) mustBe Some(RawHeader(ETAG, s""""$etagDigest1""""))
      getHeader(headers, CONTENT_MD5) mustBe Some(RawHeader(CONTENT_MD5, s"$md5Digest1"))
      getHeader(headers, VersionIdHeader) mustBe Some(RawHeader(VersionIdHeader, 2.toVersionId))
    }
  }

  it should "create different buckets" in {
    Put(s"/$bucket2") ~> routes ~> check {
      status mustBe OK
      headers.head mustBe Location(s"/$bucket2")
    }
    Put(s"/$bucket3") ~> routes ~> check {
      status mustBe OK
      headers.head mustBe Location(s"/$bucket3")
    }
    val xml =
      """
        |<VersioningConfiguration xmlns="http:/.amazonaws.com/doc/2006-03-01/">
        |<Status>Enabled</Status>
        |</VersioningConfiguration>
      """.stripMargin.replaceAll(System.lineSeparator(), "")
    val entity = HttpEntity(xmlContentType, xml)
    Put(s"/$bucket3?versioning", entity) ~> routes ~> check {
      headers.head mustBe Location(s"/$bucket3")
      status mustBe OK
    }
  }

  it should "copy object between non-versioned buckets" in {
    val key = "sample.txt"
    val copySourceHeader = RawHeader("x-amz-copy-source", s"/$defaultBucketName/$key")
    Put(s"/$bucket2/$key").withHeaders(copySourceHeader :: Nil) ~> routes ~> check {
      status mustEqual OK
      val expectedResult = createCopyObjectResult(etagDigest1, headers)
      val actualResult = entityAs[CopyObjectResult].merge(expectedResult)
      expectedResult mustEqual actualResult
    }
  }

  it should "copy object between versioned buckets" in {
    val key = "sample.txt"
    val copySourceHeader = RawHeader("x-amz-copy-source", s"/$versionedBucketName/$key")
    Put(s"/$bucket3/$key").withHeaders(copySourceHeader :: Nil) ~> routes ~> check {
      status mustEqual OK
      val expectedResult = createCopyObjectResult(etagDigest1, headers)
      val actualResult = entityAs[CopyObjectResult].merge(expectedResult)
      expectedResult mustEqual actualResult
    }
  }

  it should "copy object between versioned buckets with source version id provided" in {
    val key = "sample.txt"
    val copySourceHeader = RawHeader("x-amz-copy-source", s"/$versionedBucketName/$key?versionId=${1.toVersionId}")
    Put(s"/$bucket3/$key").withHeaders(copySourceHeader :: Nil) ~> routes ~> check {
      status mustEqual OK
      val expectedResult = createCopyObjectResult(etagDigest, headers)
      val actualResult = entityAs[CopyObjectResult].merge(expectedResult)
      expectedResult mustEqual actualResult
    }
  }

  it should "copy object from non-versioned bucket to versioned bucket" in {
    val sourceKey = "sample.txt"
    val targetKey = s"input/$sourceKey"
    val copySourceHeader = RawHeader("x-amz-copy-source", s"/$defaultBucketName/$sourceKey")
    Put(s"/$bucket3/$targetKey").withHeaders(copySourceHeader :: Nil) ~> routes ~> check {
      status mustEqual OK
      val expectedResult = createCopyObjectResult(etagDigest1, headers)
      val actualResult = entityAs[CopyObjectResult].merge(expectedResult)
      expectedResult mustEqual actualResult
    }
  }

  it should "copy object from versioned bucket to non-versioned bucket" in {
    val sourceKey = "sample.txt"
    val targetKey = s"input/$sourceKey"
    val copySourceHeader = RawHeader("x-amz-copy-source", s"/$versionedBucketName/$sourceKey")
    Put(s"/$bucket2/$targetKey").withHeaders(copySourceHeader :: Nil) ~> routes ~> check {
      status mustEqual OK
      val expectedResult = createCopyObjectResult(etagDigest1, headers)
      val actualResult = entityAs[CopyObjectResult].merge(expectedResult)
      expectedResult mustEqual actualResult
    }
  }

  it should "get NoSuchBucket when source bucket doesn't exists" in {
    val key = "sample.txt"
    val copySourceHeader = RawHeader("x-amz-copy-source", s"/$nonExistentBucketName/$key")
    Put(s"/$bucket2/$key").withHeaders(copySourceHeader :: Nil) ~> routes ~> check {
      status mustEqual NotFound
      responseAs[NoSuchBucketException] mustEqual NoSuchBucketException(nonExistentBucketName)
    }
  }

  it should "get NoSuchBucket when target bucket doesn't exists" in {
    val key = "sample.txt"
    val copySourceHeader = RawHeader("x-amz-copy-source", s"/$defaultBucketName/$key")
    Put(s"/$nonExistentBucketName/$key").withHeaders(copySourceHeader :: Nil) ~> routes ~> check {
      status mustEqual NotFound
      responseAs[NoSuchBucketException] mustEqual NoSuchBucketException(nonExistentBucketName)
    }
  }

  it should "get NoSuchKey when source key doesn't exists" in {
    val key = "_sample.txt"
    val copySourceHeader = RawHeader("x-amz-copy-source", s"/$defaultBucketName/$key")
    Put(s"/$bucket2/$key").withHeaders(copySourceHeader :: Nil) ~> routes ~> check {
      status mustEqual NotFound
      responseAs[NoSuchKeyException] mustEqual NoSuchKeyException(defaultBucketName, key)
    }
  }

  it should "list bucket contents" in {
    Get(s"/$defaultBucketName?list-type=2") ~> routes ~> check {
      status mustEqual OK
      println(getContent(response))
    }
  }

  it should "set delete marker on an object" in {
    val key: String = "sample.txt"
    Delete(s"/$defaultBucketName/$key") ~> routes ~> check {
      status mustEqual NoContent
      getHeader(headers, DeleteMarkerHeader) mustBe empty
    }
  }

  it should "result in 404(NotFound) when attempt to get an item which is flag with delete marker" in {
    val key = "sample.txt"
    Get(s"/$defaultBucketName/$key") ~> routes ~> check {
      status mustEqual NotFound
      getHeader(headers, DeleteMarkerHeader) mustBe Some(RawHeader(DeleteMarkerHeader, "true"))
    }
  }

  it should "permanently delete an object" in {
    val key: String = "sample.txt"
    Delete(s"/$defaultBucketName/$key") ~> routes ~> check {
      status mustEqual NoContent
      getHeader(headers, DeleteMarkerHeader) mustBe Some(RawHeader(DeleteMarkerHeader, "true"))
    }
  }

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

  private def getContent(response: HttpResponse) =
    response.entity.dataBytes.map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue

  private def createCopyObjectResult(eTag: String, headers: Seq[HttpHeader]) =
    CopyObjectResult(
      eTag = eTag,
      maybeVersionId = getHeader(headers, VersionIdHeader).map(_.value()),
      maybeSourceVersionId = getHeader(headers, SourceVersionIdHeader).map(_.value()),
      lastModifiedDate = Instant.now()
    )
}
