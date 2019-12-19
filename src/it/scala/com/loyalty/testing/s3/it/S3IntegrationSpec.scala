package com.loyalty.testing.s3.it

import java.nio.file._
import java.time.{Instant, OffsetDateTime}
import java.util.concurrent.CompletionException

import akka.Done
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.alpakka.s3.{S3Exception => AlpakkaS3Exception}
import akka.stream.scaladsl.{FileIO, Sink}
import com.loyalty.testing.s3.actor.model.bucket
import com.loyalty.testing.s3.actor.{BucketOperationsBehavior, CopyBehavior, NotificationBehavior, ObjectOperationsBehavior}
import com.loyalty.testing.s3.it.client.S3Client
import com.loyalty.testing.s3.repositories._
import com.loyalty.testing.s3.repositories.model.Bucket
import com.loyalty.testing.s3.request.BucketVersioning
import com.loyalty.testing.s3.response.CopyObjectResult
import com.loyalty.testing.s3.service.NotificationService
import com.loyalty.testing.s3.streams.FileStream
import com.loyalty.testing.s3.{data, _}
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.{BucketVersioningStatus, S3Exception}

import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success, Try}

abstract class S3IntegrationSpec(resourceBasename: String)
  extends AnyFlatSpec
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures {

  import S3IntegrationSpec._

  private val config = ConfigFactory.load(resourceBasename)
  private val testKit = ActorTestKit("it-test", config)
  protected implicit val system: ActorSystem[_] = testKit.system
  protected implicit val settings: ITSettings = ITSettings(system.settings.config)
  private implicit val ec: ExecutionContextExecutor = system.executionContext
  private implicit val defaultPatience: PatienceConfig = PatienceConfig(timeout = Span(15, Seconds),
    interval = Span(500, Millis))
  private val objectIO = ObjectIO(FileStream())
  private lazy val database = NitriteDatabase()
  private val notificationService: NotificationService = NotificationService(settings.awsSettings)(system.toClassic)

  private val notificationActorRef: ActorRef[ShardingEnvelope[NotificationBehavior.Command]] =
    testKit.spawn(shardingEnvelopeWrapper(NotificationBehavior(database, notificationService)))
  private val objectActorRef = testKit.spawn(shardingEnvelopeWrapper(ObjectOperationsBehavior(enableNotification = false,
    objectIO, database, notificationActorRef)))
  private val bucketOperationsActorRef: ActorRef[ShardingEnvelope[bucket.Command]] =
    testKit.spawn(shardingEnvelopeWrapper(BucketOperationsBehavior(database, objectActorRef)))
  private val copyActorRef: ActorRef[ShardingEnvelope[CopyBehavior.Command]] =
    testKit.spawn(shardingEnvelopeWrapper(CopyBehavior(bucketOperationsActorRef)))
  private val httpServer = HttpServer(database, bucketOperationsActorRef, copyActorRef, notificationActorRef)
  protected val s3Client: S3Client

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    dateTimeProvider.currentOffsetDateTime = OffsetDateTime.now()
    new Thread(() => httpServer.start()).start()
    Thread.sleep(1000)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    testKit.stop(notificationActorRef)
    testKit.stop(objectActorRef)
    testKit.stop(copyActorRef)
    testKit.stop(bucketOperationsActorRef)
    database.close()
    // clean(rootPath)
    Files.delete(rootPath -> settings.dbSettings.fileName)
    system.terminate()
  }

  it should "create bucket in default region" in {
    val bucket = s3Client.createBucket(defaultBucketName).futureValue
    bucket mustEqual Bucket(defaultBucketName, defaultRegion, BucketVersioning.NotExists)
  }

  it should "get `BucketAlreadyExists` if attempt to create bucket, which is already exists" in {
    val ex = s3Client.createBucket(defaultBucketName).failed.futureValue
    extractErrorResponse(ex) mustEqual AwsError(400, "The specified bucket already exist", "BucketAlreadyExists")
  }

  it should "create bucket with region provided" in {
    val region = Region.US_WEST_1
    val bucket = s3Client.createBucket(versionedBucketName, Some(region)).futureValue
    bucket mustEqual Bucket(versionedBucketName, region.id(), BucketVersioning.NotExists)
  }

  it should "set versioning on the bucket" in {
    s3Client.setBucketVersioning(versionedBucketName, BucketVersioningStatus.ENABLED).futureValue mustEqual Done
  }

  it should "get NoSuchBucket if attempt to set bucket versioning, which does not exist" in {
    val ex = s3Client.setBucketVersioning(nonExistentBucketName, BucketVersioningStatus.ENABLED).failed.futureValue
    extractErrorResponse(ex) mustEqual AwsError(404, "The specified bucket does not exist", "NoSuchBucket")
  }

  it should "put an object in the specified non-version bucket" in {
    val key = "sample.txt"
    val path = resourcePath -> key
    val actualObjectInfo = s3Client.putObject(defaultBucketName, key, path).futureValue
    val expectedObjectInfo = data.ObjectInfo(defaultBucketName, key, etagDigest, Files.size(path))
    actualObjectInfo mustEqual expectedObjectInfo
  }

  it should "update object in non-version bucket" in {
    val key = "sample.txt"
    val path = resourcePath -> "sample1.txt"
    val actualObjectInfo = s3Client.putObject(defaultBucketName, key, path).futureValue
    val expectedObjectInfo = data.ObjectInfo(defaultBucketName, key, etagDigest1, Files.size(path))
    actualObjectInfo mustEqual expectedObjectInfo
  }

  it should "put a multi-path object in the specified non-version bucket" in {
    val fileName = "sample.txt"
    val key = s"input/$fileName"
    val path = resourcePath -> fileName
    val actualObjectInfo = s3Client.putObject(defaultBucketName, key, path).futureValue
    val expectedObjectInfo = data.ObjectInfo(defaultBucketName, key, etagDigest, Files.size(path))
    actualObjectInfo mustEqual expectedObjectInfo
  }

  it should "get NoSuchBucket if attempt to put object in non-existing bucket" in {
    val fileName = "sample.txt"
    val key = s"input/$fileName"
    val path = resourcePath -> fileName
    val ex = s3Client.putObject(nonExistentBucketName, key, path).failed.futureValue
    extractErrorResponse(ex).copy(statusCode = 404) mustEqual AwsError(404, "The specified bucket does not exist", "NoSuchBucket")
  }

  it should "put an object in the specified bucket with bucket versioning on" in {
    val key = "sample.txt"
    val path = resourcePath -> key
    val index = 1
    val actualObjectKey = s3Client.putObject(versionedBucketName, key, path).futureValue
    val expectedObjectInfo = data.ObjectInfo(versionedBucketName, key, etagDigest, Files.size(path), Some(index.toVersionId))
    actualObjectKey mustEqual expectedObjectInfo
  }

  it should "update object in the specified bucket with bucket versioning on" in {
    val key = "sample.txt"
    val path = resourcePath -> "sample1.txt"
    val index = 2
    val actualObjectInfo = s3Client.putObject(versionedBucketName, key, path).futureValue
    val expectedObjectKey = data.ObjectInfo(versionedBucketName, key, etagDigest1, Files.size(path), Some(index.toVersionId))
    actualObjectInfo mustEqual expectedObjectKey
  }

  it should "get entire object when range is not specified" in {
    val key = "sample.txt"
    val path = resourcePath -> "sample1.txt"
    val expectedContent = FileIO.fromPath(path).map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    val expectedObjectInfo = data.ObjectInfo(defaultBucketName, key, etagDigest1, Files.size(path))
    val (actualContent, actualObjectInfo) = s3Client.getObject(defaultBucketName, key).futureValue
    actualContent mustEqual expectedContent
    actualObjectInfo mustEqual expectedObjectInfo
  }

  it should "get object with range between two positions from the start of file" in {
    val key = "sample.txt"
    val expectedContent = "1. A quick brown fox jumps over the silly lazy dog.\r\n"
    val expectedObjectInfo = data.ObjectInfo(defaultBucketName, key, etagDigest1, expectedContent.length)
    val range = ByteRange(0, 53)
    val (actualContent, actualObjectKey) = s3Client.getObject(defaultBucketName, key, maybeRange = Some(range)).futureValue
    actualContent mustEqual expectedContent
    actualObjectKey mustEqual expectedObjectInfo
  }

  it should "get object with range between two positions from the middle of file" in {
    val key = "sample.txt"
    val expectedContent = "6. A quick brown fox jumps over the silly lazy dog.\r\n"
    val expectedObjectInfo = data.ObjectInfo(defaultBucketName, key, etagDigest1, expectedContent.length)
    val range = ByteRange(265, 318)
    val (actualContent, actualObjectInfo) = s3Client.getObject(defaultBucketName, key, maybeRange = Some(range)).futureValue
    actualContent mustEqual expectedContent
    actualObjectInfo mustEqual expectedObjectInfo
  }

  it should "get object with suffix range" in {
    val key = "sample.txt"
    val expectedContent = "8. A quick brown fox jumps over the silly lazy dog.\r\n"
    val expectedObjectInfo = data.ObjectInfo(defaultBucketName, key, etagDigest1, expectedContent.length)
    val range = ByteRange.suffix(53)
    val (actualContent, actualObjectInfo) = s3Client.getObject(defaultBucketName, key, maybeRange = Some(range)).futureValue
    actualContent mustEqual expectedContent
    actualObjectInfo mustEqual expectedObjectInfo
  }

  it should "get object with range with offset" in {
    val key = "sample.txt"
    val expectedContent = "8. A quick brown fox jumps over the silly lazy dog.\r\n"
    val expectedObjectInfo = data.ObjectInfo(defaultBucketName, key, etagDigest1, expectedContent.length)
    val range = ByteRange.fromOffset(371)
    val (actualContent, actualObjectInfo) = s3Client.getObject(defaultBucketName, key, maybeRange = Some(range)).futureValue
    actualContent mustEqual expectedContent
    actualObjectInfo mustEqual expectedObjectInfo
  }

  it should "get latest object from versioned bucket without providing version" in {
    val key = "sample.txt"
    val path = resourcePath -> "sample1.txt"
    val expectedContent = FileIO.fromPath(path).map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    val index = 2
    val expectedObjectInfo = data.ObjectInfo(versionedBucketName, key, etagDigest1, expectedContent.length, Some(index.toVersionId))
    val (actualContent, actualObjectKey) = s3Client.getObject(versionedBucketName, key).futureValue
    actualContent mustEqual expectedContent
    actualObjectKey mustEqual expectedObjectInfo
  }

  it should "get object from versioned repository with version provided" in {
    val key = "sample.txt"
    val path = resourcePath -> key
    val expectedContent = FileIO.fromPath(path).map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    val index = 1
    val expectedObjectInfo = data.ObjectInfo(versionedBucketName, key, etagDigest, expectedContent.length, Some(index.toVersionId))
    val (actualContent, actualObjectInfo) = s3Client.getObject(versionedBucketName, key, maybeVersionId = Some(index.toVersionId)).futureValue
    actualContent mustEqual expectedContent
    actualObjectInfo mustEqual expectedObjectInfo
  }

  it should "get NoSuchKey for non-existing key" in {
    val key = "sample2.txt"
    val ex = s3Client.getObject(defaultBucketName, key).failed.futureValue
    extractErrorResponse(ex) mustEqual AwsError(404, "The resource you requested does not exist", "NoSuchKey")
  }

  it should "get NoSuchKey when getObject is called on a bucket which does not have versioning on but version id provided" in {
    val key = "sample.txt"
    val ex = s3Client.getObject(defaultBucketName, key, Some(NonVersionId)).failed.futureValue
    extractErrorResponse(ex) mustEqual AwsError(404, "The resource you requested does not exist", "NoSuchKey")
  }

  it should "create different buckets" in {
    s3Client.createBucket(otherBucket1).futureValue.bucketName mustEqual otherBucket1
    s3Client.createBucket(otherBucket2).futureValue.bucketName mustEqual otherBucket2
    s3Client.setBucketVersioning(otherBucket2, BucketVersioningStatus.ENABLED).futureValue
  }

  it should "copy object between non-versioned buckets" in {
    val key = "sample.txt"
    val lastModified = Instant.now()
    val actualResult = s3Client.copyObject(defaultBucketName, key, otherBucket1, key).futureValue
      .copy(lastModifiedDate = lastModified)
    val expectedResult = CopyObjectResult(etagDigest1, lastModifiedDate = lastModified)
    actualResult mustEqual expectedResult
  }

  it should "copy object between versioned buckets" in {
    val key = "sample.txt"
    val lastModified = Instant.now()
    val actualResult = s3Client.copyObject(versionedBucketName, key, otherBucket2, key).futureValue
      .copy(lastModifiedDate = lastModified)
    val expectedResult = CopyObjectResult(etagDigest1, Some(1.toVersionId), Some(2.toVersionId), lastModifiedDate = lastModified)
    actualResult mustEqual expectedResult
  }
  it should "copy object between versioned buckets with source version id provided" in {
    val key = "sample.txt"
    val lastModified = Instant.now()
    val actualResult = s3Client.copyObject(versionedBucketName, key, otherBucket2, key, Some(1.toVersionId)).futureValue
      .copy(lastModifiedDate = lastModified)
    val expectedResult = CopyObjectResult(etagDigest, Some(2.toVersionId), Some(1.toVersionId), lastModifiedDate = lastModified)
    actualResult mustEqual expectedResult
  }

  it should "copy object from non-versioned bucket to versioned bucket" in {
    val sourceKey = "sample.txt"
    val targetKey = s"input/$sourceKey"
    val lastModified = Instant.now()
    val actualResult = s3Client.copyObject(defaultBucketName, sourceKey, otherBucket2, targetKey).futureValue
      .copy(lastModifiedDate = lastModified)
    val expectedResult = CopyObjectResult(etagDigest1, Some(1.toVersionId), None, lastModifiedDate = lastModified)
    actualResult mustEqual expectedResult
  }

  it should "copy object from versioned bucket to non-versioned bucket" in {
    val sourceKey = "sample.txt"
    val targetKey = s"input/$sourceKey"
    val lastModified = Instant.now()
    val actualResult = s3Client.copyObject(versionedBucketName, sourceKey, otherBucket1, targetKey).futureValue
      .copy(lastModifiedDate = lastModified)
    val expectedResult = CopyObjectResult(etagDigest1, None, Some(2.toVersionId), lastModifiedDate = lastModified)
    actualResult mustEqual expectedResult
  }

  it should "get NoSuchBucket when source bucket doesn't exists" in {
    val key = "sample.txt"
    val ex = s3Client.copyObject(nonExistentBucketName, key, otherBucket1, key).failed.futureValue
    extractErrorResponse(ex) mustEqual AwsError(404, "The specified bucket does not exist", "NoSuchBucket")
  }

  it should "get NoSuchBucket when target bucket doesn't exists" in {
    val key = "sample.txt"
    val ex = s3Client.copyObject(defaultBucketName, key, nonExistentBucketName, key).failed.futureValue
    extractErrorResponse(ex) mustEqual AwsError(404, "The specified bucket does not exist", "NoSuchBucket")
  }

  it should "multipart upload an object" in {
    val key = "big-sample.txt"
    val objectInfo = s3Client.multiPartUpload(defaultBucketName, key, 205000).futureValue
    println(objectInfo) // TODO: validate
  }

  it should "multi part copy an object" in {
    val key = "big-sample.txt"
    val actualResult = s3Client.multiPartCopy(defaultBucketName, key, otherBucket1, key, None).futureValue
    println(actualResult) // TODO: validate
  }

  it should "set delete marker on an object" in {
    val key = "sample.txt"
    val (maybeDeleteMarker, maybeVersionId) = s3Client.deleteObject(defaultBucketName, key).futureValue
    maybeDeleteMarker mustBe empty
    maybeVersionId mustBe empty
  }

  it should "result in 404(NotFound) when attempt to get an item which is flag with delete marker" in {
    val key = "sample.txt"
    val ex = s3Client.getObject(defaultBucketName, key).failed.futureValue
    extractErrorResponse(ex).statusCode mustEqual 404
  }

  it should "permanently delete an object" in {
    val key = "sample.txt"
    val (maybeDeleteMarker, maybeVersionId) = s3Client.deleteObject(defaultBucketName, key).futureValue
    maybeDeleteMarker mustBe Some(true)
    maybeVersionId mustBe empty
  }

  it should "set delete marker on latest object on a versioned bucket" in {
    val key = "sample.txt"
    val (maybeDeleteMarker, maybeVersionId) = s3Client.deleteObject(versionedBucketName, key).futureValue
    maybeDeleteMarker mustBe empty
    maybeVersionId mustBe Some(2.toVersionId)
  }

  it should "set delete marker on by version id" in {
    val key = "sample.txt"
    val index = 1
    val (maybeDeleteMarker, maybeVersionId) = s3Client.deleteObject(versionedBucketName, key, Some(index.toVersionId)).futureValue
    maybeDeleteMarker mustBe empty
    maybeVersionId mustBe Some(index.toVersionId)
  }

  @scala.annotation.tailrec
  private def extractErrorResponse(ex: Throwable): AwsError = {
    ex match {
      case e@(_: S3Exception) =>
        val details = e.awsErrorDetails()
        AwsError(e.statusCode(), details.errorMessage(), details.errorCode())
      case e@(_: AlpakkaS3Exception) =>
        val code = e.code
        val statusCode =
          Try(code.toInt) match {
            case Failure(_) =>
              Try(code.take(3).toInt) match {
                case Failure(_) =>
                  code match {
                    case "NoSuchBucket" | "NoSuchKey" => 404
                    case _ => -1
                  }
                case Success(value) => value
              }
            case Success(value) => value
          }
        AwsError(statusCode, e.message, code)
      case e@(_: CompletionException) => extractErrorResponse(e.getCause)
      case _ =>
        ex.printStackTrace()
        AwsError(503, ex.getMessage, "Unknown")
    }
  }
}

object S3IntegrationSpec {

  case class AwsError(statusCode: Int, message: String, errorCode: String)

}
