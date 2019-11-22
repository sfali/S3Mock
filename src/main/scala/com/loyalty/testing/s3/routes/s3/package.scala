package com.loyalty.testing.s3.routes

import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.util.Timeout
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.BucketOperationsBehavior.BucketProtocol
import com.loyalty.testing.s3.actor.SpawnBehavior.{Command, Spawn}
import com.loyalty.testing.s3.actor.{BucketOperationsBehavior, Event}
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}

import scala.concurrent.Future

package object s3 {

  def spawnBucketBehavior(bucketName: String,
                          objectIO: ObjectIO,
                          database: NitriteDatabase)
                         (implicit system: ActorSystem[Command],
                          timeout: Timeout): Future[ActorRef[BucketProtocol]] =
    system.ask[ActorRef[BucketProtocol]](Spawn(BucketOperationsBehavior(objectIO, database),
      bucketName.toUUID.toString, _))

  def askBucketBehavior(actorRef: ActorRef[BucketProtocol],
                        toProtocol: ActorRef[Event] => BucketProtocol)
                       (implicit system: ActorSystem[Command],
                        timeout: Timeout): Future[Event] = actorRef.ask[Event](toProtocol)
}
