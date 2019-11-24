package com.loyalty.testing.s3.route

import java.nio.file.{Path, Paths}

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.amazonaws.auth.{AWSCredentialsProvider, AWSStaticCredentialsProvider, AnonymousAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.util.IOUtils
import com.loyalty.testing.s3.it.client.AwsClientOld
import com.loyalty.testing.s3.it.{AwsSettings, S3Settings}
import com.loyalty.testing.s3.notification.Notification
import com.loyalty.testing.s3.repositories.{FileRepository, FileStore}
import com.loyalty.testing.s3.response.ErrorCodes
import com.typesafe.config.ConfigFactory
import javax.xml.bind.DatatypeConverter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.must.Matchers

import scala.util.{Failure, Success, Try}

class S3MockAwsClientSpec
  extends TestKit(ActorSystem("it-system", ConfigFactory.load("it")))
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with ScalaFutures
    with Matchers {

  import ErrorCodes._
  import com.loyalty.testing.s3._

  private val log = system.log
  private val root: Path = Paths.get(System.getProperty("user.dir"), "tmp", "s3mock")
  private val fileStore: FileStore = FileStore(root)
  private implicit val repository: FileRepository = FileRepository(fileStore, log)
  private val s3Client = AwsClientOld(S3MockAwsClientSpec.AwsSettingsImpl)

  private val defaultBucketName = "data-transfer"
  private val s3Mock = S3Mock(fileStore)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    s3Mock.start()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    s3Mock.shutdown()
    TestKit.shutdownActorSystem(system)
  }

  it should "create bucket" in {
    val result = s3Client.createBucket(defaultBucketName)
    Option(result) must not be empty
    result.getName must equal(defaultBucketName)
  }

  it should "put bucket notification" in {
    val notification = Notification(destinationName = "queue1", bucketName = defaultBucketName,
      prefix = Some("input/"), suffix = Some(".txt"))
    s3Client.setBucketNotification(defaultBucketName, notification)
  }

  it should "not create bucket which is already exists" in {
    Try(s3Client.createBucket(defaultBucketName)) match {
      case Success(result) => Option(result) mustBe empty
      case Failure(ex: AmazonS3Exception) =>
        ex.getErrorResponseXml must include regex BucketAlreadyExists
      case Failure(ex) => throw ex
    }
  }

  it should "create bucket and set versioning" in {
    val bucketName = "other-bucket"
    val result = s3Client.createBucket(bucketName)
    result.getName must equal(bucketName)
    s3Client.setBucketVersioning(bucketName)
  }

  /*it should "create bucket with a specified region" in {
    val result = s3Client.createBucket("data-transfer-2", Region.US_West)
    Option(result) must not be empty
    result.getName must equal("data-transfer-2")
  }*/

  it should "upload multipart" in {
    val key = "input/upload.txt"
    val path = Paths.get("src", "it", "resources", "sample.txt").toAbsolutePath
    val completeResult = s3Client.multipartUpload(defaultBucketName, key, path)
    log.info("Multipart complete {}:{}", completeResult.getETag, completeResult.getVersionId)
  }

  it should "put object" in {
    val key = "input/upload1.txt"
    val path = Paths.get("src", "it", "resources", "sample.txt").toAbsolutePath
    val result = s3Client.putObject(defaultBucketName, key, path)
    val v = DatatypeConverter.printHexBinary(result.getETag.getBytes)
    log.info("{} : {} : {}", result.getETag, result.getContentMd5, v)
  }

  it should "copy object" in {
    val key = "output/upload1.txt"
    val sourceKey = "input/upload1.txt"
    val result = s3Client.copyObject(defaultBucketName, key, defaultBucketName, sourceKey)
    log.info("Copy = {}: {}", result.getETag, result.getVersionId)
  }

  it should "get object" in {
    val key = "input/upload.txt"
    val s3Object = s3Client.getObject(defaultBucketName, key)
    val s3ObjectInputStream = s3Object.getObjectContent

    Try(IOUtils.toString(s3ObjectInputStream)) match {
      case Success(s) =>
        s.length mustEqual 5365959
        s3Object.getObjectMetadata.getContentLength mustEqual 5365959
        s3ObjectInputStream.close()
      case Failure(ex) =>
        s3ObjectInputStream.close()
        fail(ex)
    }
  }

  it should "..." in {
    val key = "input/upload.txt"
    val metadata = s3Client.getObjectMetadata(defaultBucketName, key)
    log.info("((((( {}:{} )))))", metadata.getContentLength, metadata.getVersionId)
  }

  it should "get NoSuchBucket error" in {
    val path = Paths.get("src", "it", "resources", "sample.txt").toAbsolutePath
    Try(s3Client.putObject("dummy", "dummy", path)) match {
      case Failure(ex) =>
        ex match {
          case e: AmazonS3Exception =>
            log.error("Message: {}, Xml: {}", ex.getMessage, e.getErrorResponseXml)
          case _ => ex.printStackTrace()
        }
      case Success(result) => log.info(">>>>>>>>>>>>>>>> {}", result)
    }
  }

}

object S3MockAwsClientSpec {

  private case object AwsSettingsImpl extends AwsSettings {
    override val region: String = "us-east-1"
    override val credentialsProvider: AWSCredentialsProvider =
      new AWSStaticCredentialsProvider(new AnonymousAWSCredentials())

    override object s3Settings extends S3Settings {
      override val endPoint: Option[AwsClientBuilder.EndpointConfiguration] =
        Some(new EndpointConfiguration("http://localhost:9090/", region))
    }

  }

}
