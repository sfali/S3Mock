package com.loyalty.testing.s3.actor

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}

object SpawnBehavior {

  def apply(): Behavior[Command] =
    Behaviors.receive { (ctx, msg) =>
      msg match {
        case Spawn(behavior, name, replyTo) =>
          val ref =
            ctx.child(name) match {
              case Some(actorRef) => actorRef.asInstanceOf[ActorRef[Any]]
              case None => ctx.spawn(behavior, name)
            }
          replyTo ! ref
          Behaviors.same
      }
    }

  sealed trait Command

  final case class Spawn[T](behavior: Behavior[T],
                            name: String,
                            replyTo: ActorRef[ActorRef[T]]) extends Command

}


