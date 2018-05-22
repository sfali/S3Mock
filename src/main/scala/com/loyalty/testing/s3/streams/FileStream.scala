package com.loyalty.testing.s3.streams

import java.nio.ByteBuffer
import java.nio.channels.AsynchronousFileChannel
import java.nio.file.{Files, Path, StandardOpenOption}

import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.scaladsl.{Broadcast, FileIO, Flow, GraphDSL, Keep, Merge, Sink, Source}
import akka.stream.{ActorMaterializer, FlowShape, IOResult}
import akka.util.ByteString

import scala.concurrent.Future
import scala.util.Try

class FileStream private(implicit mat: ActorMaterializer) {

  import com.loyalty.testing.s3._

  def saveContent(contentSource: Source[ByteString, _], destinationPath: Path): Future[String] =
    contentSource
      .via(saveAndCalculateDigest(destinationPath))
      .toMat(Sink.head)(Keep.right)
      .run()

  def saveAndCalculateDigest(destinationPath: Path,
                             digestCalculator: DigestCalculator = DigestCalculator())
  : Flow[ByteString, String, Future[IOResult]] =
    Flow.fromGraph(GraphDSL.create(FileIO.toPath(destinationPath)) {
      implicit builder =>
        sink =>
          import GraphDSL.Implicits._

          val broadcast = builder.add(Broadcast[ByteString](2))
          val merge = builder.add(Merge[String](1))

          broadcast ~> digestCalculator ~> merge
          broadcast ~> sink

          FlowShape(broadcast.in, merge.out)
    })

  def mergeFiles(destinationPath: Path, paths: List[Path]): Future[String] =
    Source.fromIterator(() => paths.toIterator)
      .flatMapConcat(path => FileIO.fromPath(path))
      .via(saveAndCalculateDigest(destinationPath))
      .toMat(Sink.head)(Keep.right)
      .run()

  def downloadFile(path: Path, chunkSize: Int = 8192, maybeRange: Option[ByteRange] = None):
                  (DownloadRange, Source[ByteString, Future[IOResult]]) = {
    val downloadRange = DownloadRange(path, maybeRange)
    val source = Source.fromGraph(RangeDownloadSource(path, chunkSize, downloadRange))
    (downloadRange, source)
  }

  def copyPart(sourcePath: Path, destinationPath: Path,
               maybeSourceRange: Option[ByteRange.Slice] = None): Future[String] = {
    import mat.executionContext

    val (chunkSize, startPosition) =
      maybeSourceRange.map {
        range => ((range.last - range.first + 1).toInt, range.first)
      }.getOrElse((Files.size(sourcePath).toInt, 0L))

    val readChannel = AsynchronousFileChannel.open(sourcePath, StandardOpenOption.READ)
    val readBuffer = ByteBuffer.allocate(chunkSize)

    val writeChannel = AsynchronousFileChannel.open(destinationPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE)

    Future.successful(readChannel.read(readBuffer, startPosition).get())
      .flatMap {
        _ =>
          val bs = ByteString.fromArray(readBuffer.array())
          val eTag = md5Hex(bs.toArray)
          val writeBuffer = ByteBuffer.wrap(bs.toArray)
          Future.successful(writeChannel.write(writeBuffer, 0).get())
            .map {
              _ =>
                readBuffer.clear()
                writeBuffer.clear()
                Try(readChannel.close())
                Try(writeChannel.close())
                eTag
            }
      }
      .recover {
        case ex =>
          readBuffer.clear()
          Try(readChannel.close())
          Try(writeChannel.close())
          throw ex
      }
  }

}

object FileStream {
  def apply()(implicit mat: ActorMaterializer): FileStream = new FileStream()
}
