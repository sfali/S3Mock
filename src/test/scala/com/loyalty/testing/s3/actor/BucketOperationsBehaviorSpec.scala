package com.loyalty.testing.s3.actor

import java.io.IOException
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.time.OffsetDateTime

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.FileIO
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.BucketOperationsBehavior._
import com.loyalty.testing.s3.notification.{DestinationType, Notification, NotificationType, OperationType}
import com.loyalty.testing.s3.repositories._
import com.loyalty.testing.s3.repositories.model.{Bucket, ObjectKey}
import com.loyalty.testing.s3.request.{BucketVersioning, VersioningConfiguration}
import com.loyalty.testing.s3.streams.FileStream
import com.loyalty.testing.s3.test._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

class BucketOperationsBehaviorSpec
  extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll {

  import BucketOperationsBehaviorSpec._
  import BucketVersioning._

  private val testKit = ActorTestKit("test")

  private implicit val system: ActorSystem[Nothing] = testKit.system

  private val objectIO = ObjectIO(rootPath, FileStream())
  private val database = NitriteDatabase(dBSettings)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    dateTimeProvider.currentOffsetDateTime = OffsetDateTime.now()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    database.close()
    clean()
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
}

object BucketOperationsBehaviorSpec {
  private val userDir: String = System.getProperty("user.dir")
  private val rootPath: Path = Paths.get(userDir, "target", ".s3mock")
  private val resourcePath = Paths.get("src", "test", "resources")
  private val defaultBucketName = "actor-non-version"
  private val defaultBucketNameUUID = defaultBucketName.toUUID.toString
  private val versionedBucketName = "actor-with-version"
  private val versionedBucketNameUUID = versionedBucketName.toUUID.toString
  private val nonExistentBucketName = "dummy"
  private val nonExistentBucketUUID = nonExistentBucketName.toUUID.toString

  private val dBSettings: DBSettings = new DBSettings {
    override val filePath: String = (rootPath -> "s3mock.db").toString
  }

  private val etagDigest = "6b4bb2a848f1fac797e320d7b9030f3e"
  private val md5Digest = "a0uyqEjx+seX4yDXuQMPPg=="

  private def clean() =
    Files.walkFileTree(rootPath, new SimpleFileVisitor[Path] {
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        Files.delete(file)
        FileVisitResult.CONTINUE
      }

      override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
        Files.delete(dir)
        FileVisitResult.CONTINUE
      }
    })
}
