package com.loyalty.testing.s3.actor

import java.net.URI
import java.nio.file._
import java.time.OffsetDateTime
import java.util.UUID

import akka.actor.testkit.typed.scaladsl.{ActorTestKit, TestProbe}
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.scaladsl.{FileIO, Sink}
import com.loyalty.testing.s3.actor.CopyBehavior.{Copy, CopyPart}
import com.loyalty.testing.s3.actor.NotificationBehavior.{CreateBucketNotifications, GetBucketNotifications}
import com.loyalty.testing.s3.actor.model._
import com.loyalty.testing.s3.actor.model.bucket._
import com.loyalty.testing.s3.notification.{DestinationType, Notification, NotificationType, OperationType}
import com.loyalty.testing.s3.repositories.model.{Bucket, ObjectStatus}
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.request.{BucketVersioning, ObjectIdentifier, PartInfo, VersioningConfiguration}
import com.loyalty.testing.s3.response.{DeleteError, DeletedObject}
import com.loyalty.testing.s3.service.NotificationService
import com.loyalty.testing.s3.settings.Settings
import com.loyalty.testing.s3.streams.FileStream
import com.loyalty.testing.s3.test._
import com.loyalty.testing.s3.{data, _}
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import software.amazon.awssdk.auth.credentials.{AnonymousCredentialsProvider, AwsCredentialsProvider}
import software.amazon.awssdk.regions.Region

import scala.concurrent.{ExecutionContextExecutor, Future}

class BucketOperationsBehaviorSpec
  extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  import BucketOperationsBehaviorSpec._
  import BucketVersioning._

  private val config = ConfigFactory.load("test")
  private val testKit = ActorTestKit(config.getString("app.name"), config)

  private implicit val system: ActorSystem[Nothing] = testKit.system
  private implicit val settings: Settings = AppSettings(system.settings.config)
  private implicit val ec: ExecutionContextExecutor = system.executionContext
  private implicit val defaultPatience: PatienceConfig = PatienceConfig(timeout = Span(15, Seconds),
    interval = Span(500, Millis))

  private val notificationService = NotificationService(awsSettings)(system.toClassic)
  private val objectIO = ObjectIO(FileStream())
  private val database = NitriteDatabase()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    dateTimeProvider.currentOffsetDateTime = OffsetDateTime.now()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    database.close()
    Files.delete(settings.dataDirectory -> settings.dbSettings.fileName)
    testKit.shutdownTestKit()
  }

  it should "create a bucket" in {
    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), defaultBucketNameUUID)

    actorRef ! CreateBucket(Bucket(defaultBucketName, defaultRegion, NotExists), probe.ref)
    probe.expectMessage(BucketInfo(Bucket(defaultBucketName, defaultRegion, NotExists)))

    testKit.stop(objectActorRef)
    testKit.stop(actorRef)
  }

  it should "raise BucketAlreadyExistsException when attempt to create bucket which is already exists" in {
    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), defaultBucketNameUUID)

    actorRef ! CreateBucket(Bucket(defaultBucketName, defaultRegion, NotExists), probe.ref)
    probe.expectMessage(BucketAlreadyExists(Bucket(defaultBucketName, defaultRegion, NotExists)))

    testKit.stop(objectActorRef)
    testKit.stop(actorRef)
  }

  it should "create a bucket in region other than default region" in {
    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), versionedBucketNameUUID)

    actorRef ! CreateBucket(Bucket(versionedBucketName, "us-west-1", NotExists), probe.ref)
    probe.expectMessage(BucketInfo(Bucket(versionedBucketName, "us-west-1", NotExists)))

    testKit.stop(objectActorRef)
    testKit.stop(actorRef)
  }

  it should "set versioning on a bucket" in {
    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), versionedBucketNameUUID)

    val configuration = VersioningConfiguration(Enabled)
    actorRef ! SetBucketVersioning(configuration, probe.ref)
    probe.expectMessage(BucketInfo(Bucket(versionedBucketName, "us-west-1", configuration.bucketVersioning)))

    testKit.stop(objectActorRef)
    testKit.stop(actorRef)
  }

  it should "set bucket notifications" in {
    val probe = testKit.createTestProbe[Event]()
    val bucketName = defaultBucketName
    val actorRef = testKit.spawn(NotificationBehavior(database, notificationService), defaultBucketNameUUID)

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
    val actorRef = testKit.spawn(NotificationBehavior(database, notificationService), nonExistentBucketUUID)

    val notification = Notification(
      name = "queue-notification",
      notificationType = NotificationType.ObjectCreated,
      operationType = OperationType.*,
      destinationType = DestinationType.Sqs,
      destinationName = "queue-destination",
      bucketName = nonExistentBucketName,
    )

    actorRef ! CreateBucketNotifications(notification :: Nil, probe.ref)
    val actualEvent = probe.receiveMessage().asInstanceOf[NoSuchBucketExists]
    actualEvent mustEqual NoSuchBucketExists(UUID.fromString(nonExistentBucketUUID))

    testKit.stop(actorRef)
  }

  it should "create different buckets" in {
    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef1 = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), bucket2UUID)
    val actorRef2 = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), bucket3UUID)

    actorRef1 ! CreateBucket(Bucket(bucket2, defaultRegion, NotExists), probe.ref)
    probe.expectMessage(BucketInfo(Bucket(bucket2, defaultRegion, NotExists)))

    actorRef2 ! CreateBucket(Bucket(bucket3, defaultRegion, Enabled), probe.ref)
    probe.expectMessage(BucketInfo(Bucket(bucket3, defaultRegion, Enabled)))

    testKit.stop(objectActorRef)
    testKit.stop(actorRef1)
    testKit.stop(actorRef2)
  }

  it should "put an object in the specified non-version bucket" in {
    val key = "sample.txt"
    val path = resourcePath -> key
    val contentSource = FileIO.fromPath(path)

    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), defaultBucketNameUUID)
    actorRef ! PutObjectWrapper(key, contentSource, copy = false, probe.ref)

    val expectedObjectInfo = data.ObjectInfo(defaultBucketName, key, Some(etagDigest), Files.size(path))
    val actualObjectInfo = data.ObjectInfo(probe.receiveMessage().asInstanceOf[ObjectInfo].objectKey)
    actualObjectInfo mustEqual expectedObjectInfo

    testKit.stop(objectActorRef)
    testKit.stop(actorRef)
  }

  it should "get object meta for previously saved object" in {
    val key = "sample.txt"
    val path = resourcePath -> key

    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), defaultBucketNameUUID)
    actorRef ! GetObjectMetaWrapper(key, probe.ref)

    val expectedObjectInfo = data.ObjectInfo(defaultBucketName, key, Some(etagDigest), Files.size(path))
    val actualObjectInfo = data.ObjectInfo(probe.receiveMessage().asInstanceOf[ObjectInfo].objectKey)
    actualObjectInfo mustEqual expectedObjectInfo

    testKit.stop(actorRef)
  }

  it should "update object in non-version bucket" in {
    val key = "sample.txt"
    val path = resourcePath -> "sample1.txt"
    val contentSource = FileIO.fromPath(path)

    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), defaultBucketNameUUID)
    actorRef ! PutObjectWrapper(key, contentSource, copy = false, probe.ref)

    val expectedObjectInfo = data.ObjectInfo(defaultBucketName, key, Some(etagDigest1), Files.size(path))
    val actualObjectInfo = data.ObjectInfo(probe.receiveMessage().asInstanceOf[ObjectInfo].objectKey)
    actualObjectInfo mustEqual expectedObjectInfo

    testKit.stop(objectActorRef)
    testKit.stop(actorRef)
  }

  it should "put a multi-path object in the specified non-version bucket" in {
    val fileName = "sample.txt"
    val key = s"input/$fileName"
    val path = resourcePath -> fileName
    val contentSource = FileIO.fromPath(path)

    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), defaultBucketNameUUID)
    actorRef ! PutObjectWrapper(key, contentSource, copy = false, probe.ref)

    val expectedObjectInfo = data.ObjectInfo(defaultBucketName, key, Some(etagDigest), Files.size(path))
    val actualObjectInfo = data.ObjectInfo(probe.receiveMessage().asInstanceOf[ObjectInfo].objectKey)
    actualObjectInfo mustEqual expectedObjectInfo

    testKit.stop(actorRef)
    testKit.stop(actorRef)
  }

  it should "put an object in the specified bucket with bucket versioning on" in {
    val key = "sample.txt"
    val path = resourcePath -> key
    val contentSource = FileIO.fromPath(path)

    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), versionedBucketNameUUID)
    actorRef ! PutObjectWrapper(key, contentSource, copy = false, probe.ref)

    val index = 1
    val versionId = createVersionId(createObjectId(versionedBucketName, key), index)
    val expectedObjectInfo = data.ObjectInfo(versionedBucketName, key, Some(etagDigest), Files.size(path), versionId = Some(versionId))
    val actualObjectInfo = data.ObjectInfo(probe.receiveMessage().asInstanceOf[ObjectInfo].objectKey)
    actualObjectInfo mustEqual expectedObjectInfo

    testKit.stop(actorRef)
  }

  it should "update object in the specified bucket with bucket versioning on" in {
    val key = "sample.txt"
    val path = resourcePath -> "sample1.txt"
    val contentSource = FileIO.fromPath(path)

    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), versionedBucketNameUUID)
    actorRef ! PutObjectWrapper(key, contentSource, copy = false, probe.ref)

    val index = 2
    val versionId = createVersionId(createObjectId(versionedBucketName, key), index)
    val expectedObjectInfo = data.ObjectInfo(versionedBucketName, key, Some(etagDigest1), Files.size(path), versionId = Some(versionId))
    val actualObjectInfo = data.ObjectInfo(probe.receiveMessage().asInstanceOf[ObjectInfo].objectKey)
    actualObjectInfo mustEqual expectedObjectInfo

    testKit.stop(objectActorRef)
    testKit.stop(actorRef)
  }

  it should "get entire object when range is not specified" in {
    val key = "sample.txt"
    val path = resourcePath -> "sample1.txt"
    val expectedContent = FileIO.fromPath(path).map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    val expectedObjectInfo = data.ObjectInfo(defaultBucketName, key, Some(etagDigest1), expectedContent.length)

    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), defaultBucketNameUUID)
    actorRef ! GetObjectWrapper(key, replyTo = probe.ref)

    val objectContent = probe.receiveMessage().asInstanceOf[ObjectContent]
    val actualObjectInfo = data.ObjectInfo(objectContent.objectKey)
    val actualContent = objectContent.content.map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    actualObjectInfo mustEqual expectedObjectInfo
    expectedContent mustEqual actualContent

    testKit.stop(objectActorRef)
    testKit.stop(actorRef)
  }

  it should "get object with range between two positions from the start of file" in {
    val key = "sample.txt"
    val expectedContent = "1. A quick brown fox jumps over the silly lazy dog.\r\n"
    val expectedObjectInfo = data.ObjectInfo(defaultBucketName, key, Some(etagDigest1), expectedContent.length)

    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), defaultBucketNameUUID)
    actorRef ! GetObjectWrapper(key, maybeRange = Some(ByteRange(0, 53)), replyTo = probe.ref)

    val objectContent = probe.receiveMessage().asInstanceOf[ObjectContent]
    val actualObjectInfo = data.ObjectInfo(objectContent.objectKey)
    val actualContent = objectContent.content.map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    actualObjectInfo mustEqual expectedObjectInfo
    expectedContent mustEqual actualContent

    testKit.stop(objectActorRef)
    testKit.stop(actorRef)
  }

  it should "get object with range between two positions from the middle of file" in {
    val key = "sample.txt"
    val expectedContent = "6. A quick brown fox jumps over the silly lazy dog.\r\n"
    val expectedObjectInfo = data.ObjectInfo(defaultBucketName, key, Some(etagDigest1), expectedContent.length)

    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), defaultBucketNameUUID)
    actorRef ! GetObjectWrapper(key, maybeRange = Some(ByteRange(265, 318)), replyTo = probe.ref)

    val objectContent = probe.receiveMessage().asInstanceOf[ObjectContent]
    val actualObjectInfo = data.ObjectInfo(objectContent.objectKey)
    val actualContent = objectContent.content.map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    actualObjectInfo mustEqual expectedObjectInfo
    expectedContent mustEqual actualContent

    testKit.stop(objectActorRef)
    testKit.stop(actorRef)
  }

  it should "get object with suffix range" in {
    val key = "sample.txt"
    val expectedContent = "8. A quick brown fox jumps over the silly lazy dog.\r\n"
    val expectedObjectInfo = data.ObjectInfo(defaultBucketName, key, Some(etagDigest1), expectedContent.length)

    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), defaultBucketNameUUID)
    actorRef ! GetObjectWrapper(key, maybeRange = Some(ByteRange.suffix(53)), replyTo = probe.ref)

    val objectContent = probe.receiveMessage().asInstanceOf[ObjectContent]
    val actualObjectInfo = data.ObjectInfo(objectContent.objectKey)
    val actualContent = objectContent.content.map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    actualObjectInfo mustEqual expectedObjectInfo
    expectedContent mustEqual actualContent

    testKit.stop(objectActorRef)
    testKit.stop(actorRef)
  }

  it should "get object with range with offset" in {
    val key = "sample.txt"
    val expectedContent = "8. A quick brown fox jumps over the silly lazy dog.\r\n"
    val expectedObjectInfo = data.ObjectInfo(defaultBucketName, key, Some(etagDigest1), expectedContent.length)

    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), defaultBucketNameUUID)
    actorRef ! GetObjectWrapper(key, maybeRange = Some(ByteRange.fromOffset(371)), replyTo = probe.ref)

    val objectContent = probe.receiveMessage().asInstanceOf[ObjectContent]
    val actualObjectInfo = data.ObjectInfo(objectContent.objectKey)
    val actualContent = objectContent.content.map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    actualObjectInfo mustEqual expectedObjectInfo
    expectedContent mustEqual actualContent

    testKit.stop(objectActorRef)
    testKit.stop(actorRef)
  }

  it should "get latest object from versioned bucket without providing version" in {
    val key = "sample.txt"
    val path = resourcePath -> "sample1.txt"
    val expectedContent = FileIO.fromPath(path).map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    val index = 2
    val versionId = createVersionId(createObjectId(versionedBucketName, key), index)
    val expectedObjectInfo = data.ObjectInfo(versionedBucketName, key, Some(etagDigest1), expectedContent.length, versionId = Some(versionId))

    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), versionedBucketNameUUID)
    actorRef ! GetObjectWrapper(key, replyTo = probe.ref)

    val objectContent = probe.receiveMessage().asInstanceOf[ObjectContent]
    val actualObjectInfo = data.ObjectInfo(objectContent.objectKey)
    val actualContent = objectContent.content.map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    actualObjectInfo mustEqual expectedObjectInfo
    expectedContent mustEqual actualContent

    testKit.stop(objectActorRef)
    testKit.stop(actorRef)
  }

  it should "get object from versioned repository with version provided" in {
    val key = "sample.txt"
    val path = resourcePath -> key
    val expectedContent = FileIO.fromPath(path).map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    val index = 1
    val versionId = createVersionId(createObjectId(versionedBucketName, key), index)
    val expectedObjectInfo = data.ObjectInfo(versionedBucketName, key, Some(etagDigest), expectedContent.length, versionId = Some(versionId))

    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), versionedBucketNameUUID)
    actorRef ! GetObjectWrapper(key, maybeVersionId = Some(versionId), replyTo = probe.ref)

    val objectContent = probe.receiveMessage().asInstanceOf[ObjectContent]
    val actualObjectInfo = data.ObjectInfo(objectContent.objectKey)
    val actualContent = objectContent.content.map(_.utf8String).runWith(Sink.seq).map(_.mkString("")).futureValue
    actualObjectInfo mustEqual expectedObjectInfo
    expectedContent mustEqual actualContent

    testKit.stop(objectActorRef)
    testKit.stop(actorRef)
  }

  it should
    """
      |return NoSuchKeyException when getObject is called on a bucket which does not have
      | versioning on but version id provided
    """.stripMargin.replaceAll(System.lineSeparator(), "") in {
    val key = "sample.txt"

    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), defaultBucketNameUUID)
    actorRef ! GetObjectWrapper(key, maybeVersionId = Some(UUID.randomUUID().toString), replyTo = probe.ref)
    probe.expectMessage(NoSuchKeyExists(defaultBucketName, key))

    testKit.stop(objectActorRef)
    testKit.stop(actorRef)
  }

  it should "copy object between non-versioned buckets" in {
    val key = "sample.txt"

    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val bucketOperationsActorRef = testKit.spawn(shardingEnvelopeWrapper(BucketOperationsBehavior(database, objectActorRef)))
    val actorRef = testKit.spawn(CopyBehavior(bucketOperationsActorRef), UUID.randomUUID().toString)

    actorRef ! Copy(defaultBucketName, key, bucket2, key, None, probe.ref)
    val copyObjectInfo = probe.receiveMessage().asInstanceOf[CopyObjectInfo]
    copyObjectInfo.objectKey.eTag.get mustEqual etagDigest1
    copyObjectInfo.sourceVersionId mustBe empty

    testKit.stop(objectActorRef)
    testKit.stop(bucketOperationsActorRef)
    testKit.stop(actorRef)
  }

  it should "multipart upload an object" in {
    val key = "big-sample.txt"

    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), defaultBucketNameUUID)

    actorRef ! InitiateMultiPartUploadWrapper(key, probe.ref)
    val event = probe.receiveMessage().asInstanceOf[MultiPartUploadedInitiated]
    val uploadId = event.uploadId
    createUploadId(defaultBucketName, key, NotExists, 0) mustEqual uploadId

    val partNumbers = 1 :: 2 :: 3 :: Nil
    val partBoundaries = (1, 100000) :: (100001, 100000) :: (200001, 5000) :: Nil
    val parts = verifyUploads(key, uploadId, partNumbers, partBoundaries, actorRef, probe)
    actorRef ! CompleteUploadWrapper(key, uploadId, parts, probe.ref)
    val actualObjectInfo = data.ObjectInfo(probe.receiveMessage().asInstanceOf[ObjectInfo].objectKey)
    val (etag, contentLength) = calculateETagAndLength(partBoundaries)
    val expectedObjectInfo = data.ObjectInfo(defaultBucketName, key, Some(etag), contentLength)
    actualObjectInfo mustEqual expectedObjectInfo

    testKit.stop(objectActorRef)
    testKit.stop(actorRef)
  }

  it should "get object with part number" in {
    val key = "big-sample.txt"

    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), defaultBucketNameUUID)

    actorRef ! GetObjectWrapper(key, maybePartNumber = Some(2), replyTo = probe.ref)
    val objectContent = probe.receiveMessage().asInstanceOf[ObjectContent]

    val actualContent = getContentSource(objectContent.content).futureValue
    val expectedContent = getContentSource(100001, 100000).futureValue
    actualContent mustEqual expectedContent
    val actualObjectInfo = data.ObjectInfo(objectContent.objectKey)
    val digestInfo = calculateDigest(100001, 100000).futureValue
    val expectedObjectInfo = data.ObjectInfo(
      bucketName = defaultBucketName,
      key = key,
      eTag = Some(digestInfo.etag),
      contentLength = digestInfo.length
    )
    actualObjectInfo mustEqual expectedObjectInfo

    testKit.stop(objectActorRef)
    testKit.stop(actorRef)
  }

  it should "multi part copy an object" in {
    val key = "big-sample.txt"

    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val bucketOperationsActorRef = testKit.spawn(shardingEnvelopeWrapper(BucketOperationsBehavior(database, objectActorRef)))
    val actorRef = testKit.spawn(CopyBehavior(bucketOperationsActorRef), UUID.randomUUID().toString)

    bucketOperationsActorRef ! ShardingEnvelope(bucket2UUID, InitiateMultiPartUploadWrapper(key, probe.ref))
    val event = probe.receiveMessage().asInstanceOf[MultiPartUploadedInitiated]
    val uploadId = event.uploadId
    createUploadId(bucket2, key, NotExists, 0) mustEqual uploadId

    val range1 = ByteRange(0, chunkSize)
    actorRef ! CopyPart(defaultBucketName, key, bucket2, key, uploadId, 1, Some(range1), None, probe.ref)
    val partInfo1 = probe.receiveMessage().asInstanceOf[CopyPartInfo]
    partInfo1.uploadInfo.uploadId mustEqual uploadId
    partInfo1.uploadInfo.partNumber mustEqual 1
    partInfo1.sourceVersionId mustBe empty

    val range2 = ByteRange(chunkSize, chunkSize * 2)
    actorRef ! CopyPart(defaultBucketName, key, bucket2, key, uploadId, 2, Some(range2), None, probe.ref)
    val partInfo2 = probe.receiveMessage().asInstanceOf[CopyPartInfo]
    partInfo2.uploadInfo.uploadId mustEqual uploadId
    partInfo2.uploadInfo.partNumber mustEqual 2
    partInfo2.sourceVersionId mustBe empty

    val range3 = ByteRange.fromOffset(chunkSize * 2)
    actorRef ! CopyPart(defaultBucketName, key, bucket2, key, uploadId, 3, Some(range3), None, probe.ref)
    val partInfo3 = probe.receiveMessage().asInstanceOf[CopyPartInfo]
    partInfo3.uploadInfo.uploadId mustEqual uploadId
    partInfo3.uploadInfo.partNumber mustEqual 3
    partInfo3.sourceVersionId mustBe empty

    val parts = PartInfo(1, partInfo1.uploadInfo.eTag) :: PartInfo(2, partInfo2.uploadInfo.eTag) ::
      PartInfo(3, partInfo3.uploadInfo.eTag) :: Nil
    bucketOperationsActorRef ! ShardingEnvelope(bucket2UUID, CompleteUploadWrapper(key, uploadId, parts, probe.ref))
    val actualObjectKey = probe.receiveMessage().asInstanceOf[ObjectInfo].objectKey.copy(contentMd5 = None)

    bucketOperationsActorRef ! ShardingEnvelope(defaultBucketNameUUID, GetObjectWrapper(key, replyTo = probe.ref))
    val objectContent = probe.receiveMessage().asInstanceOf[ObjectContent]

    actualObjectKey.contentLength mustEqual objectContent.objectKey.contentLength

    testKit.stop(objectActorRef)
    testKit.stop(bucketOperationsActorRef)
    testKit.stop(actorRef)
  }

  it should "list bucket contents with default params" in {
    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), defaultBucketNameUUID)

    actorRef ! ListBucket(replyTo = probe.ref)
    val contents = probe.receiveMessage().asInstanceOf[ListBucketContent].contents
    println(contents) // TODO: verify

    testKit.stop(objectActorRef)
    testKit.stop(actorRef)
  }

  it should "delete an object from non-version bucket" in {
    val key = "sample.txt"
    val expected = data.ObjectInfo(defaultBucketName, key, status = ObjectStatus.Deleted)
    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), defaultBucketNameUUID)
    actorRef ! DeleteObjectWrapper(key, replyTo = probe.ref)
    val actual = data.ObjectInfo(probe.receiveMessage().asInstanceOf[ObjectInfo].objectKey)
    expected mustEqual actual

    testKit.stop(objectActorRef)
    testKit.stop(actorRef)
  }

  it should "set delete marker on an object in versioned bucket" in {
    val key = "sample.txt"
    val index = 3
    val versionId = createVersionId(createObjectId(versionedBucketName, key), index)
    val expected = data.ObjectInfo(versionedBucketName, key, status = ObjectStatus.DeleteMarker, versionId = Some(versionId))
    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), versionedBucketNameUUID)
    actorRef ! DeleteObjectWrapper(key, replyTo = probe.ref)
    val actual = data.ObjectInfo(probe.receiveMessage().asInstanceOf[ObjectInfo].objectKey)
    expected mustEqual actual

    testKit.stop(objectActorRef)
    testKit.stop(actorRef)
  }

  it should "Get the object which is a delete marker" in {
    val key = "sample.txt"
    val versionId = createVersionId(createObjectId(versionedBucketName, key), 3)
    val expected = data.ObjectInfo(versionedBucketName, key, status = ObjectStatus.DeleteMarker, versionId = Some(versionId))
    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), versionedBucketNameUUID)
    actorRef ! GetObjectWrapper(key, replyTo = probe.ref)
    val actual = data.ObjectInfo(probe.receiveMessage().asInstanceOf[ObjectInfo].objectKey)
    expected mustEqual actual

    testKit.stop(objectActorRef)
    testKit.stop(actorRef)
  }

  it should "not delete non-empty bucket" in {
    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), defaultBucketNameUUID)
    actorRef ! DeleteBucket(probe.ref)
    val event = probe.receiveMessage().asInstanceOf[BucketNotEmpty]
    BucketNotEmpty(defaultBucketName) mustEqual event

    testKit.stop(objectActorRef)
    testKit.stop(actorRef)
  }

  it should "delete multiple objects from bucket" in {
    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), defaultBucketNameUUID)

    val objects = ObjectIdentifier("input/sample.txt") ::
      ObjectIdentifier("big-sample.txt") ::
      ObjectIdentifier("unknown.txt") :: Nil
    actorRef ! DeleteObjects(objects, replyTo = probe.ref)
    val actualResult = probe.receiveMessage().asInstanceOf[DeleteObjectsResult]
    val expectedDeleted = DeletedObject("input/sample.txt") :: DeletedObject("big-sample.txt") :: Nil
    val expectedErrors = DeleteError("unknown.txt", "NoSuchKey", "The resource you requested does not exist") :: Nil
    expectedDeleted mustEqual actualResult.result.deleted
    expectedErrors mustEqual actualResult.result.errors

    testKit.stop(objectActorRef)
    testKit.stop(actorRef)
  }

  it should "delete empty bucket" in {
    val probe = testKit.createTestProbe[Event]()
    val objectActorRef = shardingObjectOperationsActorRef(testKit, objectIO, database, notificationService)
    val actorRef = testKit.spawn(BucketOperationsBehavior(database, objectActorRef), defaultBucketNameUUID)
    actorRef ! DeleteBucket(probe.ref)
    val event = probe.receiveMessage().asInstanceOf[BucketDeleted]
    BucketDeleted(defaultBucketName) mustEqual event

    testKit.stop(objectActorRef)
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
  private val awsSettings: AwsSettings = new AwsSettings {
    override val region: Region = Region.US_EAST_1
    override val credentialsProvider: AwsCredentialsProvider = AnonymousCredentialsProvider.create()
    override val sqsEndPoint: Option[URI] = None
    override val s3EndPoint: Option[URI] = None
    override val snsEndPoint: Option[URI] = None
  }
}
