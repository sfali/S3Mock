package com.loyalty.testing.s3.streams

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, StandardOpenOption}

import akka.Done
import akka.http.scaladsl.model.headers.ByteRange
import akka.http.scaladsl.model.headers.ByteRange.{FromOffset, Slice, Suffix}
import akka.stream.Attributes.InputBuffer
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, OutHandler}
import akka.stream.{Attributes, IOResult, Outlet, SourceShape}
import akka.util.ByteString
import com.loyalty.testing.s3.DownloadRange

import scala.annotation.tailrec
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class RangeDownloadSource(path: Path, chunkSize: Int = 8192, downloadRange: DownloadRange)
  extends GraphStageWithMaterializedValue[SourceShape[ByteString], Future[IOResult]] {

  require(chunkSize > 0, s"chunkSize '$chunkSize' must be greater than 0")
  require(Files.exists(path), s"Path '$path' does not exist")
  require(Files.isRegularFile(path), s"Path '$path' is not a regular file")
  require(Files.isReadable(path), s"Missing read permission for '$path'")

  private val out = Outlet[ByteString]("RangeDownloadSource.out")

  private val startPosition = downloadRange.startPosition
  private val endPosition = downloadRange.endPosition
  private val rangeCapacity = downloadRange.capacity

  override def shape: SourceShape[ByteString] = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[IOResult]) = {
    val ioResultPromise = Promise[IOResult]()

    val logic: GraphStageLogic with OutHandler = new GraphStageLogic(shape) with OutHandler {
      handler ⇒
      private val maxReadAhead = inheritedAttributes.getAttribute(classOf[InputBuffer], InputBuffer(16, 16)).max
      private var channel: FileChannel = _
      private var position = startPosition
      private var numOfBytesRead = 0L
      private var endOfReadEncountered = false
      private var availableChunks = Vector.empty[ByteString]

      setHandler(out, this)

      override def preStart(): Unit = {
        super.preStart()

        try {
          channel = FileChannel.open(path, StandardOpenOption.READ)
        } catch {
          case ex: Exception =>
            ioResultPromise.trySuccess(IOResult(numOfBytesRead, Failure(ex)))
            throw ex
        }
      }

      override def postStop(): Unit = {
        super.postStop()
        ioResultPromise.trySuccess(IOResult(numOfBytesRead, Success(Done)))
        Try(if (Option(channel).isDefined && channel.isOpen) channel.close())
      }

      override def onDownstreamFinish(): Unit = success()

      override def onPull(): Unit = {
        if (availableChunks.size < maxReadAhead && !endOfReadEncountered)
          availableChunks = readAhead(maxReadAhead, availableChunks)

        //if already read something and try
        if (availableChunks.nonEmpty) {
          emitMultiple(out, availableChunks.iterator, () => if (endOfReadEncountered) success() else setHandler(out, handler))
          availableChunks = Vector.empty[ByteString]
        } else if (endOfReadEncountered) success()
      }

      private def success(): Unit = {
        completeStage()
        ioResultPromise.trySuccess(IOResult(numOfBytesRead, Success(Done)))
      }

      // Blocking I/O Read
      @tailrec
      private def readAhead(maxChunk: Int, chunks: Vector[ByteString]): Vector[ByteString] = {
        if (chunks.size < maxChunk && !endOfReadEncountered) {
          val nextPosition = position + chunkSize
          val capacity =
            if (rangeCapacity <= chunkSize) {
              endOfReadEncountered = true
              rangeCapacity.toInt
            } else {
              if(nextPosition >= endPosition){
                endOfReadEncountered = true
                (endPosition - position).toInt
              } else chunkSize
            }
          val buffer = ByteBuffer.allocate(capacity)
          val readBytes =
            try {
              channel.read(buffer, position)
            } catch {
              case NonFatal(ex) =>
                failStage(ex)
                ioResultPromise.trySuccess(IOResult(numOfBytesRead, Failure(ex)))
                throw ex
            }

          buffer.flip()
          numOfBytesRead += readBytes
          position += readBytes
          val newChunks = chunks :+ ByteString.fromByteBuffer(buffer)
          buffer.clear()
          readAhead(maxChunk, newChunks)
        } else chunks
      }
    }

    (logic, ioResultPromise.future)
  }
}

object RangeDownloadSource {
  def apply(path: Path, chunkSize: Int = 8192, downloadRange: DownloadRange): RangeDownloadSource =
    new RangeDownloadSource(path, chunkSize, downloadRange)

  def fromPath(path: Path, chunkSize: Int = 8192, downloadRange: DownloadRange): Source[ByteString, Future[IOResult]] =
    Source.fromGraph(RangeDownloadSource(path, chunkSize, downloadRange))

}
