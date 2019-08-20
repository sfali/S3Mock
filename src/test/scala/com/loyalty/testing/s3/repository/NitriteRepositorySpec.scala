package com.loyalty.testing.s3.repository

import java.nio.file.{Files, Path, Paths}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.repositories.NitriteRepository
import com.loyalty.testing.s3.request.CreateBucketConfiguration
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
    TestKit.shutdownActorSystem(system)
  }

  it should "create a bucket" in {
    val bucketResponse = repository.createBucket(defaultBucketName, CreateBucketConfiguration()).futureValue
    bucketResponse.bucketName must equal(defaultBucketName)
    bucketResponse.locationConstraint must equal(defaultRegion)
    bucketResponse.maybeBucketVersioning mustBe empty
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
}
