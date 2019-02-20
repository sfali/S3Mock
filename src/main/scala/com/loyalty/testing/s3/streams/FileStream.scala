package com.loyalty.testing.s3.streams

import java.nio.file.Path

import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.scaladsl.{Broadcast, FileIO, Flow, GraphDSL, Keep, Merge, Sink, Source}
import akka.stream.{ActorMaterializer, FlowShape, IOResult}
import akka.util.ByteString

import scala.concurrent.Future

class FileStream private(implicit mat: ActorMaterializer) {

  import com.loyalty.testing.s3._

  def saveContent(contentSource: Source[ByteString, _], destinationPath: Path): Future[(String, String)] =
    contentSource
      .via(saveAndCalculateDigest(destinationPath))
      .toMat(Sink.head)(Keep.right)
      .run()

  def saveAndCalculateDigest(destinationPath: Path,
                             digestCalculator: DigestCalculator = DigestCalculator())
  : Flow[ByteString, (String, String), Future[IOResult]] =
    Flow.fromGraph(GraphDSL.create(FileIO.toPath(destinationPath)) {
      implicit builder =>
        sink =>
          import GraphDSL.Implicits._

          val broadcast = builder.add(Broadcast[ByteString](2))
          val merge = builder.add(Merge[(String, String)](1))

          broadcast ~> digestCalculator ~> merge
          broadcast ~> sink

          FlowShape(broadcast.in, merge.out)
    })

  def mergeFiles(destinationPath: Path, paths: List[Path]): Future[(String, String)] =
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

  def copyPart(sourcePath: Path,
               destinationPath: Path,
               maybeSourceRange: Option[ByteRange] = None): Future[(String, String)] = {
    val downloadSource = downloadFile(sourcePath, maybeRange = maybeSourceRange)
    saveContent(downloadSource._2, destinationPath)
  }

}

object FileStream {
  def apply()(implicit mat: ActorMaterializer): FileStream = new FileStream()
}
