package com.loyalty.testing.s3.it

import java.io.IOException
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.time.OffsetDateTime

import akka.Done
import akka.actor.typed.ActorSystem
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.SpawnBehavior
import com.loyalty.testing.s3.it.client.S3Client
import com.loyalty.testing.s3.repositories.model.Bucket
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.request.BucketVersioning
import com.loyalty.testing.s3.response.BucketAlreadyExistsException
import com.loyalty.testing.s3.streams.FileStream
import com.loyalty.testing.s3.utils.StaticDateTimeProvider
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus

abstract class S3IntegrationSpec(rootPath: Path,
                                 resourceBasename: String)
  extends AnyFlatSpec
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures {

  import S3IntegrationSpec._

  private val config = ConfigFactory.load(resourceBasename)
  protected implicit val system: ActorSystem[SpawnBehavior.Command] = ActorSystem(SpawnBehavior(),
    config.getString("app.name"), config)
  private implicit val dateTimeProvider: StaticDateTimeProvider = StaticDateTimeProvider()
  protected implicit val settings: ITSettings = ITSettings(system.settings.config)
  private implicit val defaultPatience: PatienceConfig = PatienceConfig(timeout = Span(15, Seconds),
    interval = Span(500, Millis))
  private val objectIO = ObjectIO(rootPath, FileStream())
  private lazy val database = NitriteDatabase(rootPath)
  private val httpServer = HttpServer(objectIO, database)
  protected val s3Client: S3Client

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    dateTimeProvider.currentOffsetDateTime = OffsetDateTime.now()
    new Thread(() => httpServer.start()).start()
    Thread.sleep(1000)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    database.close()
    clean(rootPath)
    system.terminate()
  }

  it should "create bucket in default region" in {
    val bucket = s3Client.createBucket(defaultBucketName).futureValue
    bucket mustEqual Bucket(defaultBucketName, defaultRegion, BucketVersioning.NotExists)
  }

  it should "send 404(BadRequest) if attempt to create bucket, which is already exists" in {
    s3Client.createBucket(defaultBucketName).failed.futureValue mustEqual BucketAlreadyExistsException(defaultBucketName)
  }

  it should "create bucket with region provided" in {
    val region = Region.US_WEST_1
    val bucket = s3Client.createBucket(versionedBucketName, Some(region)).futureValue
    bucket mustEqual Bucket(versionedBucketName, region.id(), BucketVersioning.NotExists)
  }

  it should "set versioning on the bucket" in {
    s3Client.setBucketVersioning(versionedBucketName, BucketVersioningStatus.ENABLED).futureValue mustEqual Done
  }

  private def clean(rootPath: Path): Path =
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

object S3IntegrationSpec {
  // private val resourcePath = Paths.get("src", "it", "resources")
  private val defaultBucketName = "non-versioned-bucket"
  private val versionedBucketName = "versioned-bucket"
  /*private val nonExistentBucketName = "dummy"
  private val etagDigest = "6b4bb2a848f1fac797e320d7b9030f3e"
  private val md5Digest = "a0uyqEjx+seX4yDXuQMPPg=="
  private val etagDigest1 = "84043a46fafcdc5451db399625915436"
  private val md5Digest1 = "hAQ6Rvr83FRR2zmWJZFUNg=="*/
}
