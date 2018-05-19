package com.loyalty.testing.s3.route

import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import com.amazonaws.auth.{AWSCredentialsProvider, AWSStaticCredentialsProvider, AnonymousAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.loyalty.testing.s3.it.client.AwsClient
import com.loyalty.testing.s3.it.{AwsSettings, S3Settings}
import com.loyalty.testing.s3.notification.Notification
import com.loyalty.testing.s3.repositories.{FileRepository, FileStore}
import com.loyalty.testing.s3.response.ErrorCodes
import com.typesafe.config.ConfigFactory
import org.apache.commons.io.IOUtils
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, MustMatchers}

import scala.util.{Failure, Success, Try}

class S3MockAwsClientSpec
  extends TestKit(ActorSystem("it-system", ConfigFactory.load("it")))
    with FlatSpecLike
    with BeforeAndAfterAll
    with ScalaFutures
    with MustMatchers {

  import ErrorCodes._
  import com.loyalty.testing.s3._

  private val log = system.log
  private implicit val mat: ActorMaterializer = ActorMaterializer()
  private val root: Path = Paths.get(System.getProperty("user.dir"), "tmp", "s3mock")
  private implicit val repository: FileRepository = FileRepository(FileStore(root), log)
  private val s3Client = AwsClient(S3MockAwsClientSpec.AwsSettingsImpl)

  private val defaultBucketName = "data-transfer"
  private val notification = Notification(destinationName = "queue1", bucketName = defaultBucketName,
    prefix = Some("input/"), suffix = Some(".txt"))
  private val s3Mock = S3Mock(notification :: Nil)

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

  it should "not create bucket which is already exists" in {
    Try(s3Client.createBucket(defaultBucketName)) match {
      case Success(result) => Option(result) mustBe empty
      case Failure(ex: AmazonS3Exception) =>
        ex.getErrorResponseXml must include regex BucketAlreadyExists
      case Failure(ex) => throw ex
    }
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

  it should "get object" in {
    val key = "input/upload.txt"
    val s3Object = s3Client.getObject(defaultBucketName, key)
    val s3ObjectInputStream = s3Object.getObjectContent

    Try(IOUtils.toString(s3ObjectInputStream, StandardCharsets.UTF_8)) match {
      case Success(s) => println(s.length)
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
