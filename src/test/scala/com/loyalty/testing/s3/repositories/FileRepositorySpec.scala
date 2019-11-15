package com.loyalty.testing.s3.repositories

import java.nio.file.{Files, Path, Paths}

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.scaladsl.{FileIO, Keep, Sink}
import akka.testkit.TestKit
import akka.util.ByteString
import com.loyalty.testing.s3.request.{BucketVersioning, CreateBucketConfiguration, VersioningConfiguration}
import com.loyalty.testing.s3.response.{BucketAlreadyExistsException, NoSuchKeyException}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.must.Matchers

class FileRepositorySpec
  extends TestKit(ActorSystem("test"))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  import com.loyalty.testing.s3._

  private val dataPath: Path = Paths.get(System.getProperty("user.dir"), ".s3mock")
  private val defaultBucketName = "actor-non-version"
  private val versionedBucketName = "actor-with-version"

  private val fileStore = FileStore(dataPath)
  private val repository = FileRepository(fileStore, system.log)

  private val etagDigest = "37099e6f8b99c52cd81df0041543e5b0"
  private val md5Digest = "Nwmeb4uZxSzYHfAEFUPlsA=="

  override protected def afterAll(): Unit = {
    super.afterAll()
    repository.clean()
    TestKit.shutdownActorSystem(system)
  }

  it should "create a bucket" in {

    whenReady(repository.createBucket(defaultBucketName, CreateBucketConfiguration())) {
      bucketResponse =>
        bucketResponse.bucketName must equal(defaultBucketName)
        bucketResponse.locationConstraint must equal("us-east-1")
        bucketResponse.maybeBucketVersioning mustBe empty
    }
  }

  it should "not create bucket which is already exists" in {
    val eventualResponse = repository.createBucket(defaultBucketName, CreateBucketConfiguration())

    whenReady(eventualResponse.failed) {
      ex => ex mustBe a[BucketAlreadyExistsException]
    }
  }

  it should "create a bucket in region other than default region" in {
    whenReady(repository.createBucket(versionedBucketName, CreateBucketConfiguration("us-west-1"))) {
      bucketResponse =>
        bucketResponse.bucketName must equal(versionedBucketName)
        bucketResponse.locationConstraint must equal("us-west-1")
        bucketResponse.maybeBucketVersioning mustBe empty
    }
  }

  it should "set versioning on the bucket" in {
    whenReady(repository.setBucketVersioning(versionedBucketName, VersioningConfiguration(BucketVersioning.Enabled))) {
      bucketResponse =>
        bucketResponse.bucketName must equal(versionedBucketName)
        bucketResponse.locationConstraint must equal("us-west-1")
        bucketResponse.maybeBucketVersioning.fold(fail("unable to get bucket version information")) {
          bucketVersioning => bucketVersioning must equal(BucketVersioning.Enabled)
        }
    }
  }

  it should "put an object in the specified non-version bucket" in {
    val key = "sample.txt"
    val contentSource = FileIO.fromPath(Paths.get("src", "test", "resources", key))
    whenReady(repository.putObject(defaultBucketName, key, contentSource)) {
      objectMeta =>
        val expectedPath = dataPath -> ("data", defaultBucketName, key, NonVersionId, ContentFileName)
        objectMeta.path must equal(expectedPath)
        Files.exists(expectedPath.toAbsolutePath) mustBe true
        val putObjectResult = objectMeta.result
        putObjectResult.etag must equal(etagDigest)
        putObjectResult.contentMd5 must equal(md5Digest)
        putObjectResult.maybeVersionId mustBe empty
    }
  }

  it should "put a multi-path object in the specified non-version bucket" in {
    val fileName = "sample.txt"
    val key = s"/input/$fileName"
    val contentSource = FileIO.fromPath(Paths.get("src", "test", "resources", fileName))
    whenReady(repository.putObject(defaultBucketName, key, contentSource)) {
      objectMeta =>
        val expectedPath = dataPath -> ("data", defaultBucketName, "input", fileName, NonVersionId, ContentFileName)
        objectMeta.path must equal(expectedPath)
        Files.exists(expectedPath.toAbsolutePath) mustBe true
        val putObjectResult = objectMeta.result
        putObjectResult.etag must equal(etagDigest)
        putObjectResult.contentMd5 must equal(md5Digest)
        putObjectResult.maybeVersionId mustBe empty
    }
  }

  it should "put an object in the specified bucket with bucket versioning on" in {
    val key = "sample1.txt"
    val contentSource = FileIO.fromPath(Paths.get("src", "test", "resources", key))
    whenReady(repository.putObject(versionedBucketName, key, contentSource)) {
      objectMeta =>
        val putObjectResult = objectMeta.result
        putObjectResult.maybeVersionId mustBe defined
        val expectedPath = dataPath -> ("data", versionedBucketName, key, putObjectResult.maybeVersionId.get, ContentFileName)
        objectMeta.path must equal(expectedPath)
        Files.exists(expectedPath.toAbsolutePath) mustBe true
        putObjectResult.etag must equal(etagDigest)
        putObjectResult.contentMd5 must equal(md5Digest)
    }
  }

  it should "put a multi-path object in the specified versioned bucket" in {
    val fileName = "sample1.txt"
    val key = s"/input/$fileName"
    val contentSource = FileIO.fromPath(Paths.get("src", "test", "resources", fileName))
    whenReady(repository.putObject(versionedBucketName, key, contentSource)) {
      objectMeta =>
        val putObjectResult = objectMeta.result
        putObjectResult.maybeVersionId mustBe defined
        val expectedPath = dataPath -> ("data", versionedBucketName, "input", fileName,
          putObjectResult.maybeVersionId.get, ContentFileName)
        objectMeta.path must equal(expectedPath)
        Files.exists(expectedPath.toAbsolutePath) mustBe true
        putObjectResult.etag must equal(etagDigest)
        putObjectResult.contentMd5 must equal(md5Digest)
    }
  }

  it should "initiate multi part upload in non version bucket" in {
    val fileName = "sample.txt"
    val key = s"/multipart/$fileName"
    whenReady(repository.initiateMultipartUpload(defaultBucketName, key)) {
      result =>
        Option(result.uploadId) mustBe defined
    }
  }

  it should "get entire object when range is not specified" in {
    val key = "sample.txt"
    whenReady(repository.getObject(defaultBucketName, key)) {
      response =>
        response.contentMd5 must equal(md5Digest)
        response.maybeVersionId mustBe empty
    }
  }

  it should "get object with range between two positions" in {
    val key = "sample.txt"
    whenReady(repository.getObject(defaultBucketName, key, None, Some(ByteRange(0, 48)))) {
      response =>
        whenReady(response.content.toMat(Sink.seq)(Keep.right).run()) {
          result =>
            val content = result.fold(ByteString(""))(_ ++ _).utf8String
            content must equal("A quick brown fox jumps over the silly lazy dog.")
            response.maybeVersionId mustBe empty
        }
    }
  }

  it should "get object with suffix range" in {
    val key = "sample.txt"
    whenReady(repository.getObject(defaultBucketName, key, None, Some(ByteRange.suffix(50)))) {
      response =>
        whenReady(response.content.toMat(Sink.seq)(Keep.right).run()) {
          result =>
            val content = result.fold(ByteString(""))(_ ++ _).utf8String
            content must equal("A quick brown fox jumps over the silly lazy dog.\r\n")
            response.maybeVersionId mustBe empty
        }
    }
  }

  it should "get object with range with offset" in {
    val key = "sample.txt"
    whenReady(repository.getObject(defaultBucketName, key, None, Some(ByteRange.fromOffset(300)))) {
      response =>
        whenReady(response.content.toMat(Sink.seq)(Keep.right).run()) {
          result =>
            val content = result.fold(ByteString(""))(_ ++ _).utf8String
            content must equal("A quick brown fox jumps over the silly lazy dog.\r\n")
            response.maybeVersionId mustBe empty
        }
    }
  }

  it should "get object with version provided" in {
    val key = "sample1.txt"
    val versionId = fileStore.get(versionedBucketName).get.getObject(key).get.result.maybeVersionId.get
    whenReady(repository.getObject(versionedBucketName, key, Some(versionId))) {
      response =>
        response.contentMd5 must equal(md5Digest)
        response.maybeVersionId.fold(fail("unable to get version id")) {
          vId => vId mustEqual versionId
        }
    }
  }

  it should "get object from versioned bucket without providing version" in {
    val key = "sample1.txt"
    val versionId = fileStore.get(versionedBucketName).get.getObject(key).get.result.maybeVersionId.get
    whenReady(repository.getObject(versionedBucketName, key)) {
      response =>
        response.contentMd5 must equal(md5Digest)
        response.maybeVersionId.fold(fail("unable to get version id")) {
          vId => vId mustEqual versionId
        }
    }
  }

  it should
    """
      |return NoSuchKeyException when getObject is called on a bucket which does not have
      | versioning on but version id provided
    """.stripMargin.replaceAll(System.lineSeparator(), "") in {
    val key = "sample.txt"
    val eventualResponse = repository.getObject(defaultBucketName, key, Some(toBase16FromRandomUUID))
    whenReady(eventualResponse.failed) {
      ex => ex mustBe a[NoSuchKeyException]
    }
  }

  it should "copy object from one path to another path within same bucket" in {
    val fileName = "sample1.txt"
    val sourceKey = s"$fileName"
    val destKey = s"/output/$fileName"
    val eventualResponse = repository.copyObject(versionedBucketName, destKey, versionedBucketName, sourceKey)
    whenReady(eventualResponse) {
      case (objectMetadata, copyResult) =>
        Files.exists(objectMetadata.path) mustEqual true
        copyResult.eTag mustEqual objectMetadata.result.etag
    }
  }

  it should "copy object from one path to another path between two buckets bucket" in {
    val fileName = "sample1.txt"
    val sourceKey = s"$fileName"
    val destKey = s"/output/$fileName"
    val eventualResponse = repository.copyObject(defaultBucketName, destKey, versionedBucketName, sourceKey)
    whenReady(eventualResponse) {
      case (objectMetadata, copyResult) =>
        Files.exists(objectMetadata.path) mustEqual true
        copyResult.eTag mustEqual objectMetadata.result.etag
    }
  }

}
