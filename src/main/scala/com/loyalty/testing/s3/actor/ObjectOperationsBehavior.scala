package com.loyalty.testing.s3.actor

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, StashBuffer}
import com.loyalty.testing.s3.actor.ObjectOperationsBehavior.ObjectProtocol
import com.loyalty.testing.s3.repositories.NitriteDatabase

import scala.concurrent.duration._

class ObjectOperationsBehavior(context: ActorContext[ObjectProtocol],
                               buffer: StashBuffer[ObjectProtocol],
                               database: NitriteDatabase)
  extends AbstractBehavior(context) {

  import ObjectOperationsBehavior._

  context.setReceiveTimeout(5.minutes, Shutdown)
  context.self ! InitializeSnapshot

  override def onMessage(msg: ObjectProtocol): Behavior[ObjectProtocol] =
    msg match {
      case InitializeSnapshot =>
        Behaviors.same

      case Shutdown => Behaviors.stopped
    }
}

object ObjectOperationsBehavior {

  def apply(database: NitriteDatabase): Behavior[ObjectProtocol] =
    Behaviors.setup[ObjectProtocol] { context =>
      Behaviors.withStash[ObjectProtocol](100) { buffer =>
        new ObjectOperationsBehavior(context, buffer, database)
      }
    }

  sealed trait ObjectProtocol

  private final case object Shutdown extends ObjectProtocol

  private final case object InitializeSnapshot extends ObjectProtocol

}
