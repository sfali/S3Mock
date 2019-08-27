package com.loyalty.testing.s3.repositories

import java.io.IOException
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.time.LocalDate

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.FileIO
import akka.testkit.TestKit
import akka.util.Timeout
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.notification.{DestinationType, Notification, NotificationType, OperationType}
import com.loyalty.testing.s3.request.{BucketVersioning, CreateBucketConfiguration, VersioningConfiguration}
import com.loyalty.testing.s3.response.{BucketAlreadyExistsException, NoSuchBucketException}
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
      destinationName = "some-destination",
      bucketName = defaultBucketName,
    )
    val result = repository.setBucketNotification(defaultBucketName, notification :: Nil).futureValue
    result must equal(Done)

    val maybeNotification = repository.notificationCollection
      .findNotification(defaultBucketName, "sample-notification")

    maybeNotification mustBe defined
    notification must equal(maybeNotification.get)
  }

  it should "fail set bucket notification for non-existing bucket" in {
    val notification = Notification(
      name = "sample-notification",
      notificationType = NotificationType.ObjectCreated,
      operationType = OperationType.*,
      destinationType = DestinationType.Sqs,
      destinationName = "no-destination",
      bucketName = nonExistentBucketName,
    )
    val eventualResponse = repository.setBucketNotification(nonExistentBucketName, notification :: Nil)
    whenReady(eventualResponse.failed) {
      ex => ex mustBe a[NoSuchBucketException]
    }
  }

  it should "put an object in the specified non-version bucket" in {
    val key = "sample.txt"
    val contentSource = FileIO.fromPath(Paths.get("src", "test", "resources", key))
    val objectMeta = repository.putObject(defaultBucketName, key, contentSource).futureValue

    val expectedPath = dataPath -> ("data", defaultBucketName, key, NonVersionId, ContentFileName)
    objectMeta.path must equal(expectedPath)
    Files.exists(expectedPath.toAbsolutePath) mustBe true
    val putObjectResult = objectMeta.result
    putObjectResult.etag must equal(etagDigest)
    putObjectResult.contentMd5 must equal(md5Digest)
    putObjectResult.maybeVersionId mustBe empty
    objectMeta.lastModifiedDate.toLocalDate must equal(LocalDate.now())
  }

  it should "put a multi-path object in the specified non-version bucket" in {
    val fileName = "sample.txt"
    val key = s"/input/$fileName"
    val contentSource = FileIO.fromPath(Paths.get("src", "test", "resources", fileName))
    val objectMeta = repository.putObject(defaultBucketName, key, contentSource).futureValue

    val expectedPath = dataPath -> ("data", defaultBucketName, "input", fileName, NonVersionId, ContentFileName)
    objectMeta.path must equal(expectedPath)
    Files.exists(expectedPath.toAbsolutePath) mustBe true
    val putObjectResult = objectMeta.result
    putObjectResult.etag must equal(etagDigest)
    putObjectResult.contentMd5 must equal(md5Digest)
    putObjectResult.maybeVersionId mustBe empty
  }

  it should "put an object in the specified bucket with bucket versioning on" in {
    val key = "sample1.txt"
    val contentSource = FileIO.fromPath(Paths.get("src", "test", "resources", key))
    val objectMeta = repository.putObject(versionedBucketName, key, contentSource).futureValue

    val putObjectResult = objectMeta.result
    putObjectResult.maybeVersionId mustBe defined
    val expectedPath = dataPath -> ("data", versionedBucketName, key, putObjectResult.maybeVersionId.get, ContentFileName)
    objectMeta.path must equal(expectedPath)
    Files.exists(expectedPath.toAbsolutePath) mustBe true
    putObjectResult.etag must equal(etagDigest)
    putObjectResult.contentMd5 must equal(md5Digest)
  }

  it should "put a multi-path object in the specified versioned bucket" in {
    val fileName = "sample1.txt"
    val key = s"/input/$fileName"
    val contentSource = FileIO.fromPath(Paths.get("src", "test", "resources", fileName))
    val objectMeta = repository.putObject(versionedBucketName, key, contentSource).futureValue

    val putObjectResult = objectMeta.result
    putObjectResult.maybeVersionId mustBe defined
    val expectedPath = dataPath -> ("data", versionedBucketName, "input", fileName, putObjectResult.maybeVersionId.get,
      ContentFileName)
    objectMeta.path must equal(expectedPath)
    Files.exists(expectedPath.toAbsolutePath) mustBe true
    putObjectResult.etag must equal(etagDigest)
    putObjectResult.contentMd5 must equal(md5Digest)
  }

}

object NitriteRepositorySpec {
  private val userDir: String = System.getProperty("user.dir")
  private val dataPath: Path = Paths.get(userDir, "target", ".s3mock")
  private val defaultBucketName = "actor-non-version"
  private val versionedBucketName = "actor-with-version"
  private val nonExistentBucketName = "dummy"

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
