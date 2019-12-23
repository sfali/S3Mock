package com.loyalty.testing.s3

import java.nio.file.{Path, Paths}

import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.stream.Materializer
import akka.stream.scaladsl.{Concat, Sink, Source}
import akka.util.ByteString
import com.loyalty.testing.s3.actor.model.`object`.Command
import com.loyalty.testing.s3.actor.{NotificationBehavior, ObjectOperationsBehavior}
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.response.CopyObjectResult
import com.loyalty.testing.s3.service.NotificationService
import com.loyalty.testing.s3.streams.{DigestCalculator, DigestInfo}
import com.loyalty.testing.s3.utils.StaticDateTimeProvider

import scala.concurrent.Future

package object test {

  implicit val dateTimeProvider: StaticDateTimeProvider = StaticDateTimeProvider()

  private val bucketIdentifier: String => (String, String) = bucketName => (bucketName, bucketName.toUUID.toString)

  def createContentSource(start: Int, totalSize: Int): Source[ByteString, NotUsed] =
    Source
      .repeat("A quick brown fox jumps over the silly lazy dog.")
      .take(totalSize)
      .zipWithIndex
      .map {
        case (s, index) => s"${"%06d".format(index + start)}. $s\r\n"
      }
      .map(ByteString(_))

  def calculateDigest(start: Int, totalSize: Int)
                     (implicit mat: Materializer): Future[DigestInfo] =
    calculateDigest(createContentSource(start, totalSize))

  def calculateDigest(contentSource: Source[ByteString, _]*)
                     (implicit mat: Materializer): Future[DigestInfo] =
    contentSource.toList match {
      case first :: Nil => first.via(DigestCalculator()).runWith(Sink.head)
      case first :: second :: rest =>
        Source.combine(first, second, rest: _*)(Concat(_)).via(DigestCalculator()).runWith(Sink.head)
      case Nil => Future.failed(new RuntimeException("empty source"))
    }

  def shardingEnvelopeWrapper[T](behavior: => Behavior[T]): Behavior[ShardingEnvelope[T]] =
    Behaviors.receive {
      case (ctx, envelope) =>
        val id = envelope.entityId
        val actorRef =
          ctx.child(id) match {
            case Some(value) => value.unsafeUpcast[T]
            case None => ctx.spawn(behavior, id)
          }
        actorRef ! envelope.message
        Behaviors.same
    }

  def shardingNotificationActorRef(testKit: ActorTestKit,
                                   database: NitriteDatabase,
                                   notificationService: NotificationService): ActorRef[ShardingEnvelope[NotificationBehavior.Command]] =
    testKit.spawn(shardingEnvelopeWrapper(NotificationBehavior(database, notificationService)))

  def shardingObjectOperationsActorRef(testKit: ActorTestKit,
                                       objectIO: ObjectIO,
                                       database: NitriteDatabase,
                                       notificationService: NotificationService): ActorRef[ShardingEnvelope[Command]] = {
    val notificationActorRef = shardingNotificationActorRef(testKit, database, notificationService)
    val behavior = ObjectOperationsBehavior(enableNotification = false, objectIO, database, notificationActorRef)
    testKit.spawn(shardingEnvelopeWrapper(behavior))
  }

  val resourcePath: Path = Paths.get("src", "test", "resources")
  val (defaultBucketName, defaultBucketNameUUID) = bucketIdentifier("non-versioned-bucket")
  val (versionedBucketName, versionedBucketNameUUID) = bucketIdentifier("versioned-bucket")
  val (bucket2, bucket2UUID) = bucketIdentifier("other-bucket-1")
  val (bucket3, bucket3UUID) = bucketIdentifier("other-bucket-2")
  val (nonExistentBucketName, nonExistentBucketUUID) = bucketIdentifier("dummy")
  val etagDigest: String = "6b4bb2a848f1fac797e320d7b9030f3e"
  val md5Digest: String = "a0uyqEjx+seX4yDXuQMPPg=="
  val etagDigest1: String = "84043a46fafcdc5451db399625915436"
  val md5Digest1: String = "hAQ6Rvr83FRR2zmWJZFUNg=="
  val chunkSize: Int = 5 * 1024 * 1024

  implicit class CopyObjectResultOps(src: CopyObjectResult) {
    def merge(target: CopyObjectResult): CopyObjectResult =
      src
        .copy(
          maybeVersionId = target.maybeVersionId,
          maybeSourceVersionId = target.maybeSourceVersionId,
          lastModifiedDate = target.lastModifiedDate
        )
  }

}
