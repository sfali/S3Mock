package com.loyalty.testing.s3.route

import java.nio.file.Files
import java.time.{Instant, OffsetDateTime}

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.stream.scaladsl.{FileIO, Sink}
import akka.util.Timeout
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.model.bucket
import com.loyalty.testing.s3.actor.{BucketOperationsBehavior, CopyBehavior, NotificationBehavior, ObjectOperationsBehavior}
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.request.{BucketVersioning, PartInfo}
import com.loyalty.testing.s3.response._
import com.loyalty.testing.s3.routes.{CustomMarshallers, Routes}
import com.loyalty.testing.s3.service.NotificationService
import com.loyalty.testing.s3.settings.Settings
import com.loyalty.testing.s3.streams.FileStream
import com.loyalty.testing.s3.test._
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{Assertion, BeforeAndAfterAll}

import scala.concurrent.duration._

class RoutesSpec
  extends AnyFlatSpec
    with Matchers
    with ScalatestRouteTest
    with Routes
    with CustomMarshallers
    with BeforeAndAfterAll
    with ScalaFutures {

  private val config: Config = ConfigFactory.load("routes")
  private val testKit = ActorTestKit(config.getString("app.name"), config)
  protected implicit val spawnSystem: ActorSystem[_] = testKit.system
  protected override implicit val timeout: Timeout = Timeout(10.seconds)
  private implicit val settings: Settings = AppSettings(spawnSystem.settings.config)
  private implicit val defaultPatience: PatienceConfig = PatienceConfig(timeout = Span(15, Seconds),
    interval = Span(500, Millis))
  private implicit val testTimeout: RouteTestTimeout = RouteTestTimeout(5.seconds)
  private val objectIO: ObjectIO = ObjectIO(FileStream())
  private val database: NitriteDatabase = NitriteDatabase()
  private val notificationService: NotificationService = NotificationService(settings.awsSettings)

  override protected val notificationActorRef: ActorRef[ShardingEnvelope[NotificationBehavior.Command]] =
    testKit.spawn(shardingEnvelopeWrapper(NotificationBehavior(database, notificationService)))
  private val objectActorRef = testKit.spawn(shardingEnvelopeWrapper(ObjectOperationsBehavior(enableNotification = false,
    objectIO, database, notificationActorRef)))
  override protected val bucketOperationsActorRef: ActorRef[ShardingEnvelope[bucket.Command]] =
    testKit.spawn(shardingEnvelopeWrapper(BucketOperationsBehavior(database, objectActorRef)))
  override protected val copyActorRef: ActorRef[ShardingEnvelope[CopyBehavior.Command]] =
    testKit.spawn(shardingEnvelopeWrapper(CopyBehavior(bucketOperationsActorRef)))
  private val xmlContentType = ContentType(MediaTypes.`application/xml`, HttpCharsets.`UTF-8`)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    dateTimeProvider.currentOffsetDateTime = OffsetDateTime.now()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    testKit.stop(notificationActorRef)
    testKit.stop(objectActorRef)
    testKit.stop(copyActorRef)
    testKit.stop(bucketOperationsActorRef)
    database.close()
    Files.delete(settings.dataDirectory -> settings.dbSettings.fileName)
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
      responseAs[BucketAlreadyExistsResponse] mustEqual BucketAlreadyExistsResponse(defaultBucketName)
    }
  }

  it should "create bucket with region provided" in {
    val xml =
      """
        |<CreateBucketConfiguration xmlns="http:/.amazonaws.com/doc/2006-03-01/">
        |<LocationConstraint>us-west-1</LocationConstraint>
        |</CreateBucketConfiguration>
       """.stripMargin.replaceNewLine
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
      """.stripMargin.replaceNewLine
    val entity = HttpEntity(xmlContentType, xml)
    Put(s"/$versionedBucketName?versioning", entity) ~> routes ~> check {
      headers.head mustBe Location(s"/$versionedBucketName")
      status mustBe OK
    }
  }

  it should "set bucket notifications" in {
    val xml =
      """<NotificationConfiguration>
        |<QueueConfiguration>
        |<Id>queue-notification</Id>
        |<Filter>
        |<S3Key>
        |<FilterRule>
        |<Name>prefix</Name>
        |<Value>input/</Value>
        |</FilterRule>
        |<FilterRule>
        |<Name>suffix</Name>
        |<Value>.T/</Value>
        |</FilterRule>
        |</S3Key>
        |</Filter>
        |<Queue>queue-destination</Queue>
        |<Event>s3:ObjectCreated:Put</Event>
        |</QueueConfiguration>
        |<TopicConfiguration>
        |<Id>topic-notification</Id>
        |<Topic>sns-destination</Topic>
        |<Event>s3:ObjectRemoved:DeleteMarkerCreated</Event>
        |</TopicConfiguration>
        |</NotificationConfiguration>""".stripMargin.replaceNewLine
    val entity = HttpEntity(xmlContentType, xml)
    Put(s"/$defaultBucketName?notification", entity) ~> routes ~> check {
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
      getHeader(headers, CONTENT_MD5) mustBe Some(RawHeader(CONTENT_MD5, md5Digest))
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
      getHeader(headers, CONTENT_MD5) mustBe Some(RawHeader(CONTENT_MD5, md5Digest1))
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
      getHeader(headers, CONTENT_MD5) mustBe Some(RawHeader(CONTENT_MD5, md5Digest))
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
      responseAs[NoSuchBucketResponse] mustEqual NoSuchBucketResponse(nonExistentBucketName)
    }
  }

  it should "get entire object when range is not specified" in {
    val key = "sample.txt"
    val path = resourcePath -> "sample1.txt"
    val expectedContent = FileIO.fromPath(path).map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    Get(s"/$defaultBucketName/$key") ~> routes ~> check {
      status mustEqual OK
      getHeader(headers, ETAG) mustBe Some(RawHeader(ETAG, s""""$etagDigest1""""))
      getHeader(headers, CONTENT_MD5) mustBe Some(RawHeader(CONTENT_MD5, md5Digest1))
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
      getHeader(headers, CONTENT_MD5) mustBe Some(RawHeader(CONTENT_MD5, md5Digest1))
      response.entity.contentLengthOption mustBe Some(53)
      expectedContent mustEqual getContent(response)
    }
  }

  it should "put an object in the specified bucket with bucket versioning on" in {
    val key = "sample.txt"
    val path = resourcePath -> key
    val versionId = createVersionId(createObjectId(versionedBucketName, key), 1)
    val contentSource = FileIO.fromPath(path)
    val entity = HttpEntity(`application/octet-stream`, contentSource)
    Put(s"/$versionedBucketName/$key", entity) ~> routes ~> check {
      status mustEqual OK
      getHeader(headers, ETAG) mustBe Some(RawHeader(ETAG, s""""$etagDigest""""))
      getHeader(headers, CONTENT_MD5) mustBe Some(RawHeader(CONTENT_MD5, md5Digest))
      getHeader(headers, VersionIdHeader) mustBe Some(RawHeader(VersionIdHeader, versionId))
    }
  }

  it should "update object in the specified bucket with bucket versioning on" in {
    val key = "sample.txt"
    val path = resourcePath -> "sample1.txt"
    val versionId = createVersionId(createObjectId(versionedBucketName, key), 2)
    val contentSource = FileIO.fromPath(path)
    val entity = HttpEntity(`application/octet-stream`, contentSource)
    Put(s"/$versionedBucketName/$key", entity) ~> routes ~> check {
      status mustEqual OK
      getHeader(headers, ETAG) mustBe Some(RawHeader(ETAG, s""""$etagDigest1""""))
      getHeader(headers, CONTENT_MD5) mustBe Some(RawHeader(CONTENT_MD5, md5Digest1))
      getHeader(headers, VersionIdHeader) mustBe Some(RawHeader(VersionIdHeader, versionId))
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
    val versionId = createVersionId(createObjectId(versionedBucketName, key), 1)
    val copySourceHeader = RawHeader("x-amz-copy-source", s"/$versionedBucketName/$key?versionId=$versionId")
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
      responseAs[NoSuchBucketResponse] mustEqual NoSuchBucketResponse(nonExistentBucketName)
    }
  }

  it should "get NoSuchBucket when target bucket doesn't exists" in {
    val key = "sample.txt"
    val copySourceHeader = RawHeader("x-amz-copy-source", s"/$defaultBucketName/$key")
    Put(s"/$nonExistentBucketName/$key").withHeaders(copySourceHeader :: Nil) ~> routes ~> check {
      status mustEqual NotFound
      responseAs[NoSuchBucketResponse] mustEqual NoSuchBucketResponse(nonExistentBucketName)
    }
  }

  it should "get NoSuchKey when source key doesn't exists" in {
    val key = "_sample.txt"
    val copySourceHeader = RawHeader("x-amz-copy-source", s"/$defaultBucketName/$key")
    Put(s"/$bucket2/$key").withHeaders(copySourceHeader :: Nil) ~> routes ~> check {
      status mustEqual NotFound
      responseAs[NoSuchKeyResponse] mustEqual NoSuchKeyResponse(defaultBucketName, key)
    }
  }

  it should "list bucket contents" in {
    Get(s"/$defaultBucketName?list-type=2") ~> routes ~> check {
      status mustEqual OK
      println(getContent(response))
    }
  }

  it should "multipart upload an object" in {
    val key = "big-sample.txt"
    val uploadId = initiateMultiPartUpload(defaultBucketName, key)

    val partInfos = uploadPart(defaultBucketName, key, uploadId, 1, 1, 90395) ::
      uploadPart(defaultBucketName, key, uploadId, 2, 90396, 90395) ::
      uploadPart(defaultBucketName, key, uploadId, 3, 180791, 24210) :: Nil

    completeMultipartUpload(defaultBucketName, key, uploadId, partInfos)
  }

  it should "multi part copy an object" in {
    val key = "big-sample.txt"
    val uploadId = initiateMultiPartUpload(bucket2, key)

    val partInfos = copyPart(bucket2, key, defaultBucketName, uploadId, 1, 0, 5242910) ::
      copyPart(bucket2, key, defaultBucketName, uploadId, 2, 5242910, 10485820) ::
      copyPart(bucket2, key, defaultBucketName, uploadId, 3, 10485820, 11890000) :: Nil

    completeMultipartUpload(bucket2, key, uploadId, partInfos)
  }

  private def initiateMultiPartUpload(bucketName: String,
                                      key: String,
                                      version: BucketVersioning = BucketVersioning.NotExists,
                                      index: Int = 0): String = {
    val uploadId = createUploadId(bucketName, key, version, index)
    val expected = InitiateMultipartUploadResult(bucketName, key, uploadId)
    Post(s"/$bucketName/$key?uploads") ~> routes ~> check {
      status mustEqual OK
      responseAs[InitiateMultipartUploadResult] mustEqual expected
    }
    uploadId
  }

  private def uploadPart(bucketName: String,
                         key: String,
                         uploadId: String,
                         partNumber: Int,
                         start: Int,
                         totalSize: Int) = {
    val entity = HttpEntity(`application/octet-stream`, createContentSource(start, totalSize))
    val etag = calculateDigest(start, totalSize).futureValue.etag
    Put(s"/$bucketName/$key?partNumber=$partNumber&uploadId=$uploadId", entity) ~> routes ~> check {
      status mustEqual OK
      val maybeETagHeader = getHeader(headers, ETAG)
      maybeETagHeader mustBe defined
      maybeETagHeader.get.value().drop(1).dropRight(1) mustEqual etag
    }
    PartInfo(partNumber, etag)
  }

  private def copyPart(bucketName: String,
                       key: String,
                       sourceBucketName: String,
                       uploadId: String,
                       partNumber: Int,
                       start: Int,
                       end: Int) = {
    val headers = RawHeader("x-amz-copy-source", s"/$sourceBucketName/$key") ::
      RawHeader("x-amz-copy-source-range", s"bytes=$start-$end") :: Nil
    // 58 is the length of string "nnnnnn: A quick brown fox jumps over the silly lazy dog.\r\n"
    val etag = calculateDigest((start / 58) + 1, (end - start) / 58).futureValue.etag
    val expected = CopyPartResult(etag)
    Put(s"/$bucketName/$key?partNumber=$partNumber&uploadId=$uploadId").withHeaders(headers) ~> routes ~> check {
      status mustEqual OK
      entityAs[CopyPartResult].copy(lastModifiedDate = expected.lastModifiedDate) mustEqual expected
    }
    PartInfo(partNumber, etag)
  }

  private def completeMultipartUpload(bucketName: String,
                                      key: String,
                                      uploadId: String,
                                      partInfos: List[PartInfo]): Assertion = {
    val parts = partInfos
      .map {
        partInfo =>
          s"""<Part><PartNumber>${partInfo.partNumber}</PartNumber><ETag>"${partInfo.eTag}"</ETag></Part>"""
      }.mkString("")


    val concatenatedETag =
      partInfos.foldLeft("") {
        case (agg, partInfo) => agg + partInfo.eTag
      }
    val finalETag = s"${toBase16(concatenatedETag)}-${partInfos.length}"
    val expectedResult = CompleteMultipartUploadResult(bucketName, key, finalETag, 0)
    val xml = s"<CompleteMultipartUploadResult>$parts</CompleteMultipartUploadResult>"
    Post(s"/$bucketName/$key?uploadId=$uploadId", HttpEntity(xmlContentType, xml)) ~> routes ~> check {
      status mustEqual OK
      responseAs[CompleteMultipartUploadResult] mustEqual expectedResult
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
