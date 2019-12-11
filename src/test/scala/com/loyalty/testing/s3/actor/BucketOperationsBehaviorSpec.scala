package com.loyalty.testing.s3.actor

import java.nio.file._
import java.time.OffsetDateTime
import java.util.UUID

import akka.actor.testkit.typed.scaladsl.{ActorTestKit, TestProbe}
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.scaladsl.{FileIO, Sink}
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.CopyBehavior.Copy
import com.loyalty.testing.s3.actor.model.{BucketAlreadyExists, BucketInfo, DeleteInfo, Event, MultiPartUploadedInitiated, NoSuchBucketExists, NoSuchKeyExists, NotificationsCreated, NotificationsInfo, ObjectContent, ObjectInfo, PartUploaded}
import com.loyalty.testing.s3.actor.model.bucket._
import com.loyalty.testing.s3.notification.{DestinationType, Notification, NotificationType, OperationType}
import com.loyalty.testing.s3.repositories.model.{Bucket, ObjectKey}
import com.loyalty.testing.s3.repositories.{NitriteDatabase, NonVersionId, ObjectIO}
import com.loyalty.testing.s3.request.{BucketVersioning, PartInfo, VersioningConfiguration}
import com.loyalty.testing.s3.streams.FileStream
import com.loyalty.testing.s3.test._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.{ExecutionContextExecutor, Future}

class BucketOperationsBehaviorSpec
  extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  import BucketOperationsBehaviorSpec._
  import BucketVersioning._

  private val testKit = ActorTestKit("test")

  private implicit val system: ActorSystem[Nothing] = testKit.system
  private implicit val ec: ExecutionContextExecutor = system.executionContext
  private implicit val defaultPatience: PatienceConfig = PatienceConfig(timeout = Span(15, Seconds),
    interval = Span(500, Millis))

  private val objectIO = ObjectIO(rootPath, FileStream())
  private val database = NitriteDatabase(rootPath, dBSettings)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    dateTimeProvider.currentOffsetDateTime = OffsetDateTime.now()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    database.close()
    Files.delete(rootPath -> dBSettings.fileName)
    testKit.shutdownTestKit()
  }

  it should "create a bucket" in {
    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(objectIO, database), defaultBucketNameUUID)

    actorRef ! CreateBucket(Bucket(defaultBucketName, defaultRegion, NotExists), probe.ref)
    probe.expectMessage(BucketInfo(Bucket(defaultBucketName, defaultRegion, NotExists)))

    testKit.stop(actorRef)
  }

  it should "raise BucketAlreadyExistsException when attempt to create bucket which is already exists" in {
    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(objectIO, database), defaultBucketNameUUID)

    actorRef ! CreateBucket(Bucket(defaultBucketName, defaultRegion, NotExists), probe.ref)
    probe.expectMessage(BucketAlreadyExists(Bucket(defaultBucketName, defaultRegion, NotExists)))

    testKit.stop(actorRef)
  }

  it should "create a bucket in region other than default region" in {
    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(objectIO, database), versionedBucketNameUUID)

    actorRef ! CreateBucket(Bucket(versionedBucketName, "us-west-1", NotExists), probe.ref)
    probe.expectMessage(BucketInfo(Bucket(versionedBucketName, "us-west-1", NotExists)))

    testKit.stop(actorRef)
  }

  it should "set versioning on a bucket" in {
    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(objectIO, database), versionedBucketNameUUID)

    val configuration = VersioningConfiguration(Enabled)
    actorRef ! SetBucketVersioning(configuration, probe.ref)
    probe.expectMessage(BucketInfo(Bucket(versionedBucketName, "us-west-1", configuration.bucketVersioning)))

    testKit.stop(actorRef)
  }

  it should "set bucket notifications" in {
    val probe = testKit.createTestProbe[Event]()
    val bucketName = defaultBucketName
    val actorRef = testKit.spawn(BucketOperationsBehavior(objectIO, database), defaultBucketNameUUID)

    val notification1 = Notification(
      name = "queue-notification",
      notificationType = NotificationType.ObjectCreated,
      operationType = OperationType.*,
      destinationType = DestinationType.Sqs,
      destinationName = "queue-destination",
      bucketName = bucketName,
    )
    val notification2 = Notification(
      name = "sns-notification",
      notificationType = NotificationType.ObjectCreated,
      operationType = OperationType.*,
      destinationType = DestinationType.Sns,
      destinationName = "sns-destination",
      bucketName = bucketName,
    )

    val notifications = notification1 :: notification2 :: Nil
    actorRef ! CreateBucketNotifications(notifications, probe.ref)
    probe.expectMessage(NotificationsCreated)

    actorRef ! GetBucketNotifications(probe.ref)
    probe.expectMessage(NotificationsInfo(notifications))

    testKit.stop(actorRef)
  }

  it should "fail set bucket notification for non-existing bucket" in {
    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(objectIO, database), nonExistentBucketUUID)

    val notification = Notification(
      name = "queue-notification",
      notificationType = NotificationType.ObjectCreated,
      operationType = OperationType.*,
      destinationType = DestinationType.Sqs,
      destinationName = "queue-destination",
      bucketName = nonExistentBucketName,
    )

    actorRef ! CreateBucketNotifications(notification :: Nil, probe.ref)
    probe.expectMessage(NoSuchBucketExists)

    testKit.stop(actorRef)
  }

  it should "create different buckets" in {
    val probe = testKit.createTestProbe[Event]()
    val actorRef1 = testKit.spawn(BucketOperationsBehavior(objectIO, database), bucket2UUID)
    val actorRef2 = testKit.spawn(BucketOperationsBehavior(objectIO, database), bucket3UUID)

    actorRef1 ! CreateBucket(Bucket(bucket2, defaultRegion, NotExists), probe.ref)
    probe.expectMessage(BucketInfo(Bucket(bucket2, defaultRegion, NotExists)))

    actorRef2 ! CreateBucket(Bucket(bucket3, defaultRegion, Enabled), probe.ref)
    probe.expectMessage(BucketInfo(Bucket(bucket3, defaultRegion, Enabled)))

    testKit.stop(actorRef1)
    testKit.stop(actorRef2)
  }

  it should "put an object in the specified non-version bucket" in {
    val key = "sample.txt"
    val path = resourcePath -> key
    val contentSource = FileIO.fromPath(path)

    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(objectIO, database), defaultBucketNameUUID)
    actorRef ! PutObjectWrapper(key, contentSource, probe.ref)

    val objectKey = ObjectKey(
      id = createObjectId(defaultBucketName, key),
      bucketName = defaultBucketName,
      key = key,
      index = 0,
      version = NotExists,
      versionId = NonVersionId,
      eTag = etagDigest,
      contentMd5 = md5Digest,
      contentLength = Files.size(path),
      lastModifiedTime = dateTimeProvider.currentOffsetDateTime
    )
    probe.expectMessage(ObjectInfo(objectKey))

    testKit.stop(actorRef)
  }

  it should "get object meta for previously saved object" in {
    val key = "sample.txt"
    val path = resourcePath -> key

    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(objectIO, database), defaultBucketNameUUID)
    actorRef ! GetObjectMetaWrapper(key, probe.ref)

    val expectedObjectKey = ObjectKey(
      id = createObjectId(defaultBucketName, key),
      bucketName = defaultBucketName,
      key = key,
      index = 0,
      version = NotExists,
      versionId = NonVersionId,
      eTag = etagDigest,
      contentMd5 = md5Digest,
      contentLength = Files.size(path),
      lastModifiedTime = dateTimeProvider.currentOffsetDateTime
    )
    val event: ObjectInfo = probe.receiveMessage().asInstanceOf[ObjectInfo]
    val actualObjectKey = event.objectKey.copy(lastModifiedTime = dateTimeProvider.currentOffsetDateTime)
    expectedObjectKey mustEqual actualObjectKey

    testKit.stop(actorRef)
  }

  it should "update object in non-version bucket" in {
    val key = "sample.txt"
    val path = resourcePath -> key
    val contentSource = FileIO.fromPath(path)

    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(objectIO, database), defaultBucketNameUUID)
    actorRef ! PutObjectWrapper(key, contentSource, probe.ref)

    val objectKey = ObjectKey(
      id = createObjectId(defaultBucketName, key),
      bucketName = defaultBucketName,
      key = key,
      index = 0,
      version = NotExists,
      versionId = NonVersionId,
      eTag = etagDigest,
      contentMd5 = md5Digest,
      contentLength = Files.size(path),
      lastModifiedTime = dateTimeProvider.currentOffsetDateTime
    )
    probe.expectMessage(ObjectInfo(objectKey))

    testKit.stop(actorRef)
  }

  it should "put a multi-path object in the specified non-version bucket" in {
    val fileName = "sample.txt"
    val key = s"input/$fileName"
    val path = resourcePath -> fileName
    val contentSource = FileIO.fromPath(path)

    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(objectIO, database), defaultBucketNameUUID)
    actorRef ! PutObjectWrapper(key, contentSource, probe.ref)

    val objectKey = ObjectKey(
      id = createObjectId(defaultBucketName, key),
      bucketName = defaultBucketName,
      key = key,
      index = 0,
      version = NotExists,
      versionId = NonVersionId,
      eTag = etagDigest,
      contentMd5 = md5Digest,
      contentLength = Files.size(path),
      lastModifiedTime = dateTimeProvider.currentOffsetDateTime
    )
    probe.expectMessage(ObjectInfo(objectKey))

    testKit.stop(actorRef)
  }

  it should "put an object in the specified bucket with bucket versioning on" in {
    val key = "sample.txt"
    val path = resourcePath -> key
    val contentSource = FileIO.fromPath(path)

    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(objectIO, database), versionedBucketNameUUID)
    actorRef ! PutObjectWrapper(key, contentSource, probe.ref)

    val index = 1
    val objectKey = ObjectKey(
      id = createObjectId(versionedBucketName, key),
      bucketName = versionedBucketName,
      key = key,
      index = index,
      version = Enabled,
      versionId = index.toVersionId,
      eTag = etagDigest,
      contentMd5 = md5Digest,
      contentLength = Files.size(path),
      lastModifiedTime = dateTimeProvider.currentOffsetDateTime
    )
    probe.expectMessage(ObjectInfo(objectKey))

    testKit.stop(actorRef)
  }

  it should "update object in the specified bucket with bucket versioning on" in {
    val key = "sample.txt"
    val path = resourcePath -> key
    val contentSource = FileIO.fromPath(path)

    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(objectIO, database), versionedBucketNameUUID)
    actorRef ! PutObjectWrapper(key, contentSource, probe.ref)

    val index = 2
    val objectKey = ObjectKey(
      id = createObjectId(versionedBucketName, key),
      bucketName = versionedBucketName,
      key = key,
      index = index,
      version = Enabled,
      versionId = index.toVersionId,
      eTag = etagDigest,
      contentMd5 = md5Digest,
      contentLength = Files.size(path),
      lastModifiedTime = dateTimeProvider.currentOffsetDateTime
    )
    probe.expectMessage(ObjectInfo(objectKey))

    testKit.stop(actorRef)
  }

  it should "get entire object when range is not specified" in {
    val key = "sample.txt"
    val path = resourcePath -> key
    val expectedContent = FileIO.fromPath(path).map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    val expectedObjectKey = ObjectKey(
      id = createObjectId(defaultBucketName, key),
      bucketName = defaultBucketName,
      key = key,
      index = 0,
      version = NotExists,
      versionId = NonVersionId,
      eTag = etagDigest,
      contentMd5 = md5Digest,
      contentLength = Files.size(path),
      lastModifiedTime = dateTimeProvider.currentOffsetDateTime
    )

    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(objectIO, database), defaultBucketNameUUID)
    actorRef ! GetObjectWrapper(key, replyTo = probe.ref)

    val objectContent = probe.receiveMessage().asInstanceOf[ObjectContent]
    val actualObjectKey = objectContent.objectKey.copy(lastModifiedTime = dateTimeProvider.currentOffsetDateTime)
    val actualContent = objectContent.content.map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    expectedObjectKey mustEqual actualObjectKey
    expectedContent mustEqual actualContent

    testKit.stop(actorRef)
  }

  it should "get object with range between two positions from the start of file" in {
    val key = "sample.txt"
    val expectedContent = "1. A quick brown fox jumps over the silly lazy dog.\r\n"
    val expectedObjectKey = ObjectKey(
      id = createObjectId(defaultBucketName, key),
      bucketName = defaultBucketName,
      key = key,
      index = 0,
      version = NotExists,
      versionId = NonVersionId,
      eTag = etagDigest,
      contentMd5 = md5Digest,
      contentLength = expectedContent.length,
      lastModifiedTime = dateTimeProvider.currentOffsetDateTime
    )

    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(objectIO, database), defaultBucketNameUUID)
    actorRef ! GetObjectWrapper(key, maybeRange = Some(ByteRange(0, 53)), replyTo = probe.ref)

    val objectContent = probe.receiveMessage().asInstanceOf[ObjectContent]
    val actualObjectKey = objectContent.objectKey.copy(lastModifiedTime = dateTimeProvider.currentOffsetDateTime)
    val actualContent = objectContent.content.map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    expectedObjectKey mustEqual actualObjectKey
    expectedContent mustEqual actualContent

    testKit.stop(actorRef)
  }

  it should "get object with range between two positions from the middle of file" in {
    val key = "sample.txt"
    val expectedContent = "6. A quick brown fox jumps over the silly lazy dog.\r\n"
    val expectedObjectKey = ObjectKey(
      id = createObjectId(defaultBucketName, key),
      bucketName = defaultBucketName,
      key = key,
      index = 0,
      version = NotExists,
      versionId = NonVersionId,
      eTag = etagDigest,
      contentMd5 = md5Digest,
      contentLength = expectedContent.length,
      lastModifiedTime = dateTimeProvider.currentOffsetDateTime
    )

    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(objectIO, database), defaultBucketNameUUID)
    actorRef ! GetObjectWrapper(key, maybeRange = Some(ByteRange(265, 318)), replyTo = probe.ref)

    val objectContent = probe.receiveMessage().asInstanceOf[ObjectContent]
    val actualObjectKey = objectContent.objectKey.copy(lastModifiedTime = dateTimeProvider.currentOffsetDateTime)
    val actualContent = objectContent.content.map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    expectedObjectKey mustEqual actualObjectKey
    expectedContent mustEqual actualContent

    testKit.stop(actorRef)
  }

  it should "get object with suffix range" in {
    val key = "sample.txt"
    val expectedContent = "7. A quick brown fox jumps over the silly lazy dog.\r\n"
    val expectedObjectKey = ObjectKey(
      id = createObjectId(defaultBucketName, key),
      bucketName = defaultBucketName,
      key = key,
      index = 0,
      version = NotExists,
      versionId = NonVersionId,
      eTag = etagDigest,
      contentMd5 = md5Digest,
      contentLength = expectedContent.length,
      lastModifiedTime = dateTimeProvider.currentOffsetDateTime
    )

    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(objectIO, database), defaultBucketNameUUID)
    actorRef ! GetObjectWrapper(key, maybeRange = Some(ByteRange.suffix(53)), replyTo = probe.ref)

    val objectContent = probe.receiveMessage().asInstanceOf[ObjectContent]
    val actualObjectKey = objectContent.objectKey.copy(lastModifiedTime = dateTimeProvider.currentOffsetDateTime)
    val actualContent = objectContent.content.map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    expectedObjectKey mustEqual actualObjectKey
    expectedContent mustEqual actualContent

    testKit.stop(actorRef)
  }

  it should "get object with range with offset" in {
    val key = "sample.txt"
    val expectedContent = "7. A quick brown fox jumps over the silly lazy dog.\r\n"
    val expectedObjectKey = ObjectKey(
      id = createObjectId(defaultBucketName, key),
      bucketName = defaultBucketName,
      key = key,
      index = 0,
      version = NotExists,
      versionId = NonVersionId,
      eTag = etagDigest,
      contentMd5 = md5Digest,
      contentLength = expectedContent.length,
      lastModifiedTime = dateTimeProvider.currentOffsetDateTime
    )

    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(objectIO, database), defaultBucketNameUUID)
    actorRef ! GetObjectWrapper(key, maybeRange = Some(ByteRange.fromOffset(318)), replyTo = probe.ref)

    val objectContent = probe.receiveMessage().asInstanceOf[ObjectContent]
    val actualObjectKey = objectContent.objectKey.copy(lastModifiedTime = dateTimeProvider.currentOffsetDateTime)
    val actualContent = objectContent.content.map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    expectedObjectKey mustEqual actualObjectKey
    expectedContent mustEqual actualContent

    testKit.stop(actorRef)
  }

  it should "get latest object from versioned bucket without providing version" in {
    val key = "sample.txt"
    val path = resourcePath -> key
    val expectedContent = FileIO.fromPath(path).map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    val index = 2
    val expectedObjectKey = ObjectKey(
      id = createObjectId(versionedBucketName, key),
      bucketName = versionedBucketName,
      key = key,
      index = index,
      version = Enabled,
      versionId = index.toVersionId,
      eTag = etagDigest,
      contentMd5 = md5Digest,
      contentLength = Files.size(path),
      lastModifiedTime = dateTimeProvider.currentOffsetDateTime
    )

    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(objectIO, database), versionedBucketNameUUID)
    actorRef ! GetObjectWrapper(key, replyTo = probe.ref)

    val objectContent = probe.receiveMessage().asInstanceOf[ObjectContent]
    val actualObjectKey = objectContent.objectKey.copy(lastModifiedTime = dateTimeProvider.currentOffsetDateTime)
    val actualContent = objectContent.content.map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    expectedObjectKey mustEqual actualObjectKey
    expectedContent mustEqual actualContent

    testKit.stop(actorRef)
  }

  it should "get object from versioned repository with version provided" in {
    val key = "sample.txt"
    val path = resourcePath -> key
    val expectedContent = FileIO.fromPath(path).map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    val index = 1
    val expectedObjectKey = ObjectKey(
      id = createObjectId(versionedBucketName, key),
      bucketName = versionedBucketName,
      key = key,
      index = index,
      version = Enabled,
      versionId = index.toVersionId,
      eTag = etagDigest,
      contentMd5 = md5Digest,
      contentLength = Files.size(path),
      lastModifiedTime = dateTimeProvider.currentOffsetDateTime
    )

    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(objectIO, database), versionedBucketNameUUID)
    actorRef ! GetObjectWrapper(key, maybeVersionId = Some(index.toVersionId.toString), replyTo = probe.ref)

    val objectContent = probe.receiveMessage().asInstanceOf[ObjectContent]
    val actualObjectKey = objectContent.objectKey.copy(lastModifiedTime = dateTimeProvider.currentOffsetDateTime)
    val actualContent = objectContent.content.map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    expectedObjectKey mustEqual actualObjectKey
    expectedContent mustEqual actualContent

    testKit.stop(actorRef)
  }

  it should
    """
      |return NoSuchKeyException when getObject is called on a bucket which does not have
      | versioning on but version id provided
    """.stripMargin.replaceAll(System.lineSeparator(), "") in {
    val key = "sample.txt"

    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(objectIO, database), defaultBucketNameUUID)
    actorRef ! GetObjectWrapper(key, maybeVersionId = Some(UUID.randomUUID().toString), replyTo = probe.ref)
    probe.expectMessage(NoSuchKeyExists)

    testKit.stop(actorRef)
  }

  it should "copy object between non-versioned buckets" in {
    val key = "sample.txt"

    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(CopyBehavior(objectIO, database), UUID.randomUUID().toString)

    actorRef ! Copy(defaultBucketName, key, bucket2, key, None, probe.ref)
    val objectKey = probe.receiveMessage().asInstanceOf[ObjectInfo].objectKey
    print(objectKey)

    testKit.stop(actorRef)
  }

  it should "multipart upload an object" in {
    val key = "big-sample.txt"

    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(objectIO, database), defaultBucketNameUUID)

    actorRef ! InitiateMultiPartUploadWrapper(key, probe.ref)
    val event = probe.receiveMessage().asInstanceOf[MultiPartUploadedInitiated]
    val uploadId = event.uploadId
    createUploadId(defaultBucketName, NotExists, key, 0) mustEqual uploadId

    val partNumbers = 1 :: 2 :: 3 :: Nil
    val partBoundaries = (1, 100000) :: (100001, 100000) :: (200001, 5000) :: Nil
    val parts = verifyUploads(key, uploadId, partNumbers, partBoundaries, actorRef, probe)
    actorRef ! CompleteUploadWrapper(key, uploadId, parts, probe.ref)
    val actualObjectKey = probe.receiveMessage().asInstanceOf[ObjectInfo].objectKey.copy(contentMd5 = "")
    val (etag, contentLength) = calculateETagAndLength(partBoundaries)
    val expectedObjectKey = ObjectKey(
      id = createObjectId(defaultBucketName, key),
      bucketName = defaultBucketName,
      key = key,
      index = 0,
      version = NotExists,
      versionId = NonVersionId,
      eTag = etag,
      contentMd5 = "",
      contentLength = contentLength,
      lastModifiedTime = dateTimeProvider.currentOffsetDateTime,
      uploadId = Some(uploadId)
    )
    actualObjectKey mustEqual expectedObjectKey

    testKit.stop(actorRef)
  }

  it should "set delete marker on an object" in {
    val key = "sample.txt"

    val expected = DeleteInfo(deleteMarker = false, NotExists)
    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(objectIO, database), defaultBucketNameUUID)
    actorRef ! DeleteObjectWrapper(key, replyTo = probe.ref)
    val actual = probe.receiveMessage().asInstanceOf[DeleteInfo]
    expected mustEqual actual

    testKit.stop(actorRef)
  }

  it should "delete an object" in {
    val key = "sample.txt"

    val expected = DeleteInfo(deleteMarker = true, NotExists)
    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(objectIO, database), defaultBucketNameUUID)
    actorRef ! DeleteObjectWrapper(key, replyTo = probe.ref)
    val actual = probe.receiveMessage().asInstanceOf[DeleteInfo]
    actual mustEqual expected

    testKit.stop(actorRef)
  }

  private def verifyUploadPart(key: String,
                               uploadId: String,
                               partNumber: Int,
                               start: Int,
                               totalSize: Int,
                               actorRef: ActorRef[Command],
                               probe: TestProbe[Event]) = {
    actorRef ! UploadPartWrapper(key, uploadId, partNumber, createContentSource(start, totalSize), probe.ref)
    val partUploadEvent = probe.receiveMessage().asInstanceOf[PartUploaded]
    val uploadInfo = partUploadEvent.uploadInfo
    uploadInfo.uploadId mustEqual uploadId
    uploadInfo.partNumber mustEqual partNumber
    PartInfo(partNumber, uploadInfo.eTag)
  }

  private def verifyUploads(key: String,
                            uploadId: String,
                            partNumbers: List[Int],
                            partBoundaries: List[(Int, Int)],
                            actorRef: ActorRef[Command],
                            probe: TestProbe[Event]): List[PartInfo] = {
    val ls = partNumbers.zip(partBoundaries).map(xs => (xs._1, xs._2._1, xs._2._2))
    ls.map {
      case (partNumber, start, totalSize) =>
        verifyUploadPart(key, uploadId, partNumber, start, totalSize, actorRef, probe)
    }
  }

  private def calculateETagAndLength(partBoundaries: List[(Int, Int)]): (String, Long) = {
    val (concatenatedETag, totalLength) =
      Future.sequence(
        partBoundaries.map {
          case (start, totalSize) => calculateDigest(start, totalSize)
        })
        .futureValue
        .foldLeft(("", 0L)) {
          case ((etag, length), digestInfo) => (etag + digestInfo.etag, length + digestInfo.length)
        }
    (s"${toBase16(concatenatedETag)}-${partBoundaries.length}", totalLength)
  }

}

object BucketOperationsBehaviorSpec {
  private val dBSettings: DBSettings = new DBSettings {
    override val fileName: String = "s3mock.db"
  }
}
