package com.loyalty.testing.s3.repositories

import java.io.IOException
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.notification.{DestinationType, Notification, NotificationType, OperationType}
import com.loyalty.testing.s3.request.{BucketVersioning, CreateBucketConfiguration, VersioningConfiguration}
import com.loyalty.testing.s3.response.BucketAlreadyExistsException
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, MustMatchers}

import scala.concurrent.duration._

class NitriteRepositorySpec
  extends TestKit(ActorSystem("test"))
    with FlatSpecLike
    with MustMatchers
    with BeforeAndAfterAll
    with ScalaFutures {

  import NitriteRepositorySpec._

  private implicit val mat: ActorMaterializer = ActorMaterializer()
  private implicit val defaultPatience: PatienceConfig = PatienceConfig(timeout = Span(15, Seconds),
    interval = Span(500, Millis))
  private implicit val timeout: Timeout = Timeout(5.seconds)

  private val repository = NitriteRepository(dBSettings, dataPath, system.log)

  override protected def afterAll(): Unit = {
    super.afterAll()
    repository.clean()
    clean()
    TestKit.shutdownActorSystem(system)
  }

  it should "create a bucket" in {
    val bucketResponse = repository.createBucket(defaultBucketName, CreateBucketConfiguration()).futureValue
    bucketResponse.bucketName must equal(defaultBucketName)
    bucketResponse.locationConstraint must equal(defaultRegion)
    bucketResponse.maybeBucketVersioning mustBe empty
  }

  it should "raise BucketAlreadyExistsException when attempt to create bucket which is already exists" in {
    val eventualResponse = repository.createBucket(defaultBucketName, CreateBucketConfiguration())
    whenReady(eventualResponse.failed) {
      ex => ex mustBe a[BucketAlreadyExistsException]
    }
  }

  it should "create a bucket in region other than default region" in {
    val bucketResponse = repository.createBucket(versionedBucketName, CreateBucketConfiguration("us-west-1")).futureValue
    bucketResponse.bucketName must equal(versionedBucketName)
    bucketResponse.locationConstraint must equal("us-west-1")
    bucketResponse.maybeBucketVersioning mustBe empty
  }

  it should "set versioning on a bucket" in {
    val bucketResponse = repository.setBucketVersioning(versionedBucketName,
      VersioningConfiguration(BucketVersioning.Enabled)).futureValue
    bucketResponse.bucketName must equal(versionedBucketName)
    bucketResponse.locationConstraint must equal("us-west-1")
    bucketResponse.maybeBucketVersioning.fold(fail("unable to get bucket version information")) {
      bucketVersioning => bucketVersioning must equal(BucketVersioning.Enabled)
    }
  }

  it should "set bucket notification" in {
    val notification = Notification(
      name = "sample-notification",
      notificationType = NotificationType.ObjectCreated,
      operationType = OperationType.*,
      destinationType = DestinationType.Sqs,
      destinationName = "",
      bucketName = defaultBucketName,
    )
    val result = repository.setBucketNotification(defaultBucketName, notification :: Nil).futureValue
    result must equal(Done)

    val maybeNotification = repository.notificationCollection
      .findNotification(defaultBucketName, "sample-notification")

    maybeNotification mustBe defined
    notification must equal(maybeNotification.get)
  }
}

object NitriteRepositorySpec {
  private val userDir: String = System.getProperty("user.dir")
  private val dataPath: Path = Paths.get(userDir, "target", ".s3mock")
  private val defaultBucketName = "actor-non-version"
  private val versionedBucketName = "actor-with-version"

  private val dBSettings: DBSettings = new DBSettings {
    override val filePath: String = (dataPath -> "s3mock.db").toString
  }

  private val etagDigest = "37099e6f8b99c52cd81df0041543e5b0"
  private val md5Digest = "Nwmeb4uZxSzYHfAEFUPlsA=="

  private def clean() =
    Files.walkFileTree(dataPath, new SimpleFileVisitor[Path] {
      override def visitFile(file: Path,
                             attrs: BasicFileAttributes): FileVisitResult = {
        Files.delete(file)
        FileVisitResult.CONTINUE
      }

      override def postVisitDirectory(dir: Path,
                                      exc: IOException): FileVisitResult = {
        Files.delete(dir)
        FileVisitResult.CONTINUE
      }
    })
}
