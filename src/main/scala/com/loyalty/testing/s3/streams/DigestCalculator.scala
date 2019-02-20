package com.loyalty.testing.s3.streams


import java.security.MessageDigest

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString
import com.amazonaws.util.BinaryUtils
import javax.xml.bind.DatatypeConverter

class DigestCalculator(algorithm: String) extends GraphStage[FlowShape[ByteString, (String, String)]] {

  private val in = Inlet[ByteString]("DigestCalculator.in")
  private val out = Outlet[(String, String)]("DigestCalculator.out")

  override def shape: FlowShape[ByteString, (String, String)] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private val digest = MessageDigest.getInstance(algorithm)

      setHandler(out, new OutHandler {
        override def onPull(): Unit = pull(in)
      })

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val chunk = grab(in)
          digest.update(chunk.toArray)
          pull(in)
        }

        override def onUpstreamFinish(): Unit = {
          val bytes = digest.digest()
          val etag = DatatypeConverter.printHexBinary(bytes).toLowerCase
          val contentMd5 = DatatypeConverter.printBase64Binary(bytes)
          emit(out, (etag, contentMd5))
          completeStage()
        }
      })

    }

}

object DigestCalculator {
  def apply(): DigestCalculator = DigestCalculator("MD5")

  def apply(algorithm: String): DigestCalculator = new DigestCalculator(algorithm)
}
