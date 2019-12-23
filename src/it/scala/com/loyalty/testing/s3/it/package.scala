package com.loyalty.testing.s3

import java.nio.file.{Files, Path, Paths}

import akka.NotUsed
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.stream.Materializer
import akka.stream.scaladsl.{FileIO, Sink, Source}
import akka.util.ByteString
import com.loyalty.testing.s3.streams.{DigestCalculator, DigestInfo}
import com.loyalty.testing.s3.utils.StaticDateTimeProvider

import scala.concurrent.Future

package object it {

  implicit val dateTimeProvider: StaticDateTimeProvider = StaticDateTimeProvider()

  def createContentSource(start: Int, totalSize: Int): Source[ByteString, NotUsed] =
    Source
      .repeat("A quick brown fox jumps over the silly lazy dog.")
      .take(totalSize)
      .zipWithIndex
      .map {
        case (s, index) => s"${"%06d".format(index + start)}. $s\r\n"
      }
      .map(ByteString(_))

  def saveFile(start: Int,
               totalSize: Int,
               filePrefix: String,
               fileSuffix: String)
              (implicit mat: Materializer): Future[Path] = {
    import mat.executionContext
    val path = Files.createTempFile(filePrefix, fileSuffix)
    path.toFile.deleteOnExit()
    createContentSource(start, totalSize)
      .runWith(FileIO.toPath(path))
      .map(_ => path)
  }

  def calculateDigest(start: Int, totalSize: Int)(implicit mat: Materializer): Future[DigestInfo] =
    createContentSource(start, totalSize)
      .via(DigestCalculator()).runWith(Sink.head)

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

  val resourcePath: Path = Paths.get("src", "it", "resources")
  val defaultBucketName: String = "non-versioned-bucket"
  val versionedBucketName: String = "versioned-bucket"
  val nonExistentBucketName: String = "dummy"
  val otherBucket1: String = "other-bucket-1"
  val otherBucket2: String = "other-bucket-2"
  val etagDigest: String = "6b4bb2a848f1fac797e320d7b9030f3e"
  // private val md5Digest = "a0uyqEjx+seX4yDXuQMPPg=="
  val etagDigest1: String = "84043a46fafcdc5451db399625915436"
  // private val md5Digest1 = "hAQ6Rvr83FRR2zmWJZFUNg=="
}
