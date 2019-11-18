package com.loyalty.testing.s3.actor

import java.io.IOException
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.ActorSystem
import com.loyalty.testing.s3.actor.BucketOperationsBehavior._
import com.loyalty.testing.s3.notification.{DestinationType, Notification, NotificationType, OperationType}
import com.loyalty.testing.s3.repositories.NitriteDatabase
import com.loyalty.testing.s3.repositories.model.Bucket
import com.loyalty.testing.s3.request.{BucketVersioning, VersioningConfiguration}
import com.loyalty.testing.s3.{DBSettings, _}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

class BucketOperationsBehaviorSpec
  extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  import BucketOperationsBehaviorSpec._

  private val testKit = ActorTestKit("test")

  private implicit val system: ActorSystem[Nothing] = testKit.system
  private implicit val defaultPatience: PatienceConfig = PatienceConfig(timeout = Span(15, Seconds),
    interval = Span(500, Millis))

  private val database = NitriteDatabase(dBSettings)

  override protected def afterAll(): Unit = {
    super.afterAll()
    database.close()
    clean()
    testKit.shutdownTestKit()
  }

  it should "create a bucket" in {
    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(database), defaultBucketNameUUID)

    actorRef ! CreateBucket(Bucket(defaultBucketName, defaultRegion, None), probe.ref)
    probe.expectMessage(BucketInfo(Bucket(defaultBucketName, defaultRegion, None)))

    testKit.stop(actorRef)
  }

  it should "raise BucketAlreadyExistsException when attempt to create bucket which is already exists" in {
    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(database), defaultBucketNameUUID)

    actorRef ! CreateBucket(Bucket(defaultBucketName, defaultRegion, None), probe.ref)
    probe.expectMessage(BucketAlreadyExists(Bucket(defaultBucketName, defaultRegion, None)))

    testKit.stop(actorRef)
  }

  it should "create a bucket in region other than default region" in {
    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(database), versionedBucketNameUUID)

    actorRef ! CreateBucket(Bucket(versionedBucketName, "us-west-1", None), probe.ref)
    probe.expectMessage(BucketInfo(Bucket(versionedBucketName, "us-west-1", None)))

    testKit.stop(actorRef)
  }

  it should "set versioning on a bucket" in {
    val probe = testKit.createTestProbe[Event]()
    val actorRef = testKit.spawn(BucketOperationsBehavior(database), versionedBucketNameUUID)

    val configuration = VersioningConfiguration(BucketVersioning.Enabled)
    actorRef ! SetBucketVersioning(configuration, probe.ref)
    probe.expectMessage(BucketInfo(Bucket(versionedBucketName, "us-west-1", Some(configuration.bucketVersioning))))

    testKit.stop(actorRef)
  }

  it should "set bucket notifications" in {
    val probe = testKit.createTestProbe[Event]()
    val bucketName = defaultBucketName
    val actorRef = testKit.spawn(BucketOperationsBehavior(database), defaultBucketNameUUID)

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
    val actorRef = testKit.spawn(BucketOperationsBehavior(database), nonExistentBucketUUID)

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
}

object BucketOperationsBehaviorSpec {
  private val userDir: String = System.getProperty("user.dir")
  private val dataPath: Path = Paths.get(userDir, "target", ".s3mock")
  if (!Files.exists(dataPath)) Files.createDirectories(dataPath)
  private val defaultBucketName = "actor-non-version"
  private val defaultBucketNameUUID = defaultBucketName.toUUID.toString
  private val versionedBucketName = "actor-with-version"
  private val versionedBucketNameUUID = versionedBucketName.toUUID.toString
  private val nonExistentBucketName = "dummy"
  private val nonExistentBucketUUID = nonExistentBucketName.toUUID.toString

  private val dBSettings: DBSettings = new DBSettings {
    override val filePath: String = (dataPath -> "s3mock.db").toString
  }

  private def clean() =
    Files.walkFileTree(dataPath, new SimpleFileVisitor[Path] {
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
