package com.loyalty.testing.s3

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.loyalty.testing.s3.streams.{DigestCalculator, DigestInfo}
import com.loyalty.testing.s3.utils.StaticDateTimeProvider

import scala.concurrent.Future

package object test {

  implicit val dateTimeProvider: StaticDateTimeProvider = StaticDateTimeProvider()

  def createContentSource(start: Int, totalSize: Int): Source[ByteString, NotUsed] =
    Source
      .repeat("A quick brown fox jumps over the silly lazy dog.")
      .take(totalSize)
      .zipWithIndex
      .map {
        case (s, index) => s"${index + start}. $s\r\n"
      }
      .map(ByteString(_))

  def calculateDigest(start: Int, totalSize: Int)(implicit mat: Materializer): Future[DigestInfo] =
    createContentSource(start, totalSize)
      .via(DigestCalculator()).runWith(Sink.head)


}
