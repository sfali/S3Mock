package com.loyalty.testing.s3.actor

import java.util.UUID

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, StashBuffer}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.NotificationBehavior.{SendNotification, Command => NotificationCommand}
import com.loyalty.testing.s3.actor.model.`object`._
import com.loyalty.testing.s3.actor.model.{InternalError, InvalidAccess, InvalidPart, InvalidPartOrder, MultiPartUploadedInitiated, NoSuchKeyExists, NoSuchUpload, ObjectContent, ObjectInfo, PartUploaded}
import com.loyalty.testing.s3.notification.{NotificationData, OperationType}
import com.loyalty.testing.s3.repositories.collections.NoSuchId
import com.loyalty.testing.s3.repositories.model.{Bucket, ObjectKey, ObjectStatus, UploadInfo}
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.request.BucketVersioning
import com.loyalty.testing.s3.request.BucketVersioning.Enabled
import com.loyalty.testing.s3.service._
import com.loyalty.testing.s3.settings.Settings

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class ObjectOperationsBehavior(context: ActorContext[Command],
                               buffer: StashBuffer[Command],
                               enableNotification: Boolean,
                               objectService: ObjectService,
                               notificationActorRef: ActorRef[ShardingEnvelope[NotificationCommand]])
  extends AbstractBehavior(context) {

  import ObjectOperationsBehavior._
  import OperationType._

  private implicit val ec: ExecutionContext = context.system.executionContext
  private var versionIndex = 0
  private val holder = ObjectKeyHolder()
  private var uploadInfo: Option[UploadInfo] = None
  private var uploadParts: Map[String, Set[UploadInfo]] = Map.empty
  private val objectId = UUID.fromString(context.self.path.name)
  context.setReceiveTimeout(5.minutes, Shutdown)
  context.self ! InitializeSnapshot

  override def onMessage(msg: Command): Behavior[Command] =
    msg match {
      case InitializeSnapshot =>
        context.pipeToSelf(objectService.getAllObjects(objectId)) {
          case Failure(NoSuchId(_)) => ObjectResult(Nil, Nil)
          case Failure(ex) =>
            context.log.error(s"unable to load snapshot: $objectId", ex)
            // TODO: reply properly
            Shutdown
          case Success((objects, uploads)) => ObjectResult(objects, uploads)
        }
        Behaviors.same

      case ObjectResult(objects, uploads) =>
        versionIndex = objects.lastOption.map(_.index).getOrElse(0)
        this.holder.addAll(objects)
        val uploadsMap = uploads.groupBy(_.uploadId)
        if (uploadsMap.nonEmpty) {
          val ls = uploadsMap.head._2
          uploadInfo = ls.find(_.partNumber == 0)
          uploadParts = ls.filterNot(_.partNumber == 0).toSet.groupBy(_.uploadId)
        }
        buffer.unstashAll(objectOperations)

      case Shutdown => Behaviors.stopped

      case other =>
        buffer.stash(other)
        Behaviors.same
    }

  private def objectOperations: Behavior[Command] =
    Behaviors.receiveMessagePartial {
      case protocol: ObjectInput if protocol.objectId != objectId =>
        context.self ! ReplyToSender(InvalidAccess, protocol.replyTo)
        Behaviors.same

      case protocol: UploadInput if !uploadInfo.exists(_.uploadId == protocol.uploadId) =>
        context.self ! ReplyToSender(NoSuchUpload, protocol.replyTo)
        Behaviors.same

      case PutObject(bucket, key, contentSource, copy, replyTo) =>
        versionIndex = if (BucketVersioning.Enabled == bucket.version) versionIndex + 1 else versionIndex
        context.pipeToSelf(objectService.saveObject(bucket, key, objectId, versionIndex, contentSource)) {
          case Failure(ex) =>
            context.log.error(s"unable to save object: ${bucket.bucketName}/$key", ex)
            DatabaseError
          case Success(objectKey) =>
            val operationType = if (copy) Copy else Put
            ReplyToSender(ObjectInfo(objectKey), replyTo, Some(objectKey), Some(operationType))
        }
        Behaviors.same

      case DatabaseError =>
        Behaviors.same

      case GetObject(bucket, key, maybeVersionId, maybeRange, replyTo) =>
        val maybeObjectKey = holder.getObject(sanitizeVersionId(bucket, maybeVersionId))
        val command =
          maybeObjectKey match {
            case None => ReplyToSender(NoSuchKeyExists(bucket.bucketName, key), replyTo)
            case Some(objectKey) =>
              objectKey.status match {
                case ObjectStatus.Active =>
                  Try(objectService.getObject(objectKey, maybeRange)) match {
                    case Success((updatedObjectKey, source)) =>
                      ReplyToSender(ObjectContent(updatedObjectKey, source), replyTo)
                    case Failure(ex) =>
                      context.log.error("unable to download object", ex)
                      DatabaseError // TODO: retry
                  }
                case ObjectStatus.DeleteMarker => ReplyToSender(ObjectInfo(objectKey.copy(deleteMarker = Some(true))), replyTo)
                case _ => ReplyToSender(NoSuchKeyExists(bucket.bucketName, key), replyTo)
              }
          }
        context.self ! command
        Behaviors.same

      case DeleteObject(bucket, key, maybeVersionId, replyTo) =>
        val maybeSanitizedVersionId = sanitizeVersionId(bucket, maybeVersionId)
        holder.getObject(maybeSanitizedVersionId) match {
          case Some(objectKey) =>
            (maybeSanitizedVersionId, bucket.version) match {
              case (None, Enabled) =>
                // sets delete marker
                versionIndex = versionIndex + 1
                val deleteMarkerObjectKey = ObjectKey(
                  id = objectId,
                  bucketName = bucket.bucketName,
                  key = key,
                  index = versionIndex,
                  version = Enabled,
                  versionId = createVersionId(objectId, versionIndex),
                  status = ObjectStatus.DeleteMarker
                )
                context.pipeToSelf(objectService.createOrUpdateObject(deleteMarkerObjectKey)) {
                  case Failure(ex) =>
                    context.log.error(s"unable to save object: ${bucket.bucketName}/$key", ex)
                    DatabaseError // TODO
                  case Success(finalObjectKey) =>
                    ReplyToSender(ObjectInfo(finalObjectKey), replyTo, Some(finalObjectKey), Some(DeleteMarkerCreated))
                }

              case (_, _) =>
                // virtual delete
                context.pipeToSelf(objectService.virtualDeleteObject(objectKey)) {
                  case Failure(_: NoSuchKeyException.type) =>
                    ReplyToSender(NoSuchKeyExists(bucket.bucketName, key), replyTo)
                  case Failure(ex) =>
                    context.log.error(s"unable to virtual delete object: ${bucket.bucketName}/$key", ex)
                    DatabaseError // TODO
                  case Success(finalObjectKey) =>
                    ReplyToSender(ObjectInfo(finalObjectKey), replyTo, Some(finalObjectKey), Some(Delete))
                }
            }
          case None =>
            context.log.warn("unable to delete, reason=NoSuchKey, bucket_name={}, key={}, version_id={}",
              bucket.bucketName, key, maybeVersionId.getOrElse("None"))
            context.self ! ReplyToSender(NoSuchKeyExists(bucket.bucketName, key), replyTo)
        }
        Behaviors.same

      case InitiateMultiPartUpload(bucket, key, replyTo) =>
        val bucketName = bucket.bucketName
        val version = bucket.version
        versionIndex = if (BucketVersioning.Enabled == version) versionIndex + 1 else versionIndex
        val versionId = createVersionId(objectId, versionIndex)
        val uploadId = createUploadId(bucketName, key, version, versionIndex)
        val uploadPath = toObjectDir(bucketName, key, version, versionId, Some(uploadId))
        val uploadInfo = UploadInfo(
          bucketName = bucketName,
          key = key,
          version = version,
          versionIndex = versionIndex,
          uploadId = uploadId,
          uploadPath = uploadPath
        )
        context.pipeToSelf(objectService.createUpload(uploadInfo)) {
          case Failure(ex) =>
            context.log.error(s"unable to initiated multi part upload: $bucketName/$key", ex)
            DatabaseError // TODO: retry
          case Success(_) => UploadInfoCreated(uploadInfo, replyTo)
        }
        Behaviors.same

      case UploadInfoCreated(uploadInfo, replyTo) =>
        this.uploadInfo = Some(uploadInfo)
        context.self ! ReplyToSender(MultiPartUploadedInitiated(uploadInfo.uploadId), replyTo)
        Behaviors.same

      case UploadPart(bucket, key, uploadId, partNumber, contentSource, replyTo) =>
        val partInfo = uploadInfo.get.copy(partNumber = partNumber)
        context.pipeToSelf(objectService.savePart(partInfo, contentSource)) {
          case Failure(ex) =>
            context.log.error(s"unable to create part upload: ${bucket.bucketName}/$key/$uploadId", ex)
            DatabaseError // TODO: retry
          case Success(uploadInfo) => PartSaved(uploadInfo.copy(contentMd5 = "", contentLength = 0), replyTo)
        }
        Behaviors.same

      case PartSaved(uploadInfo, replyTo) =>
        val parts = uploadParts.getOrElse(uploadInfo.uploadId, Set.empty[UploadInfo])
        uploadParts = uploadParts + (uploadInfo.uploadId -> (parts + uploadInfo))
        context.self ! ReplyToSender(PartUploaded(uploadInfo), replyTo)
        Behaviors.same

      case CompleteUpload(_, _, uploadId, parts, replyTo) =>
        val uploadInfo = this.uploadInfo.get
        context.pipeToSelf(objectService.completeUpload(uploadInfo, parts, uploadParts)) {
          case Failure(InternalServiceException) => ReplyToSender(InternalError, replyTo)
          case Failure(InvalidPartOrderException) => ReplyToSender(InvalidPartOrder, replyTo)
          case Failure(InvalidPartException(partNumber)) => ReplyToSender(InvalidPart(partNumber), replyTo)
          case Failure(ex) =>
            context.log.error(s"unable to complete upload: ${uploadInfo.bucketName}/${uploadInfo.key}/$uploadId", ex)
            DatabaseError // TODO: retry
          case Success(objectKey) => ResetUploadInfo(objectKey, replyTo)
        }
        Behaviors.same

      case ResetUploadInfo(objectKey, replyTo) =>
        uploadInfo = None
        context.self ! ReplyToSender(ObjectInfo(objectKey), replyTo, Some(objectKey), Some(CompleteMultipartUpload))
        Behaviors.same

      case GetObjectMeta(bucket, key, replyTo) =>
        val maybeObjectKey = holder.objects.find(_.id == objectId)
        val event = maybeObjectKey.map(ObjectInfo.apply).getOrElse(NoSuchKeyExists(bucket.bucketName, key))
        context.self ! ReplyToSender(event, replyTo)
        Behaviors.same

      case ReplyToSender(reply, replyTo, maybeObjectKey, maybeOperation) =>
        // send notification if applicable
        if (enableNotification && maybeOperation.isDefined && maybeObjectKey.isDefined) {
          val objectKey = maybeObjectKey.get
          val bucketName = objectKey.bucketName
          val notificationData = NotificationData(bucketName, objectKey.key,
            objectKey.contentLength, objectKey.eTag.getOrElse(""), maybeOperation.get, objectKey.actualVersionId)
          notificationActorRef ! ShardingEnvelope(bucketName.toUUID.toString, SendNotification(notificationData))
        }
        if (maybeObjectKey.isDefined) holder.add(maybeObjectKey.get)
        replyTo ! reply
        Behaviors.same

      case Shutdown => Behaviors.stopped

      case other =>
        context.log.warn("unhandled message: {}", other)
        Behaviors.unhandled
    }

}

object ObjectOperationsBehavior {

  val TypeKey: EntityTypeKey[Command] = EntityTypeKey[Command]("ObjectOperationsActor")

  def apply(enableNotification: Boolean,
            objectIO: ObjectIO,
            database: NitriteDatabase,
            notificationActorRef: ActorRef[ShardingEnvelope[NotificationCommand]]): Behavior[Command] =
    Behaviors.setup[Command] { context =>
      Behaviors.withStash[Command](1000) { buffer =>
        new ObjectOperationsBehavior(context, buffer, enableNotification, ObjectService(objectIO, database),
          notificationActorRef)
      }
    }

  def init(sharding: ClusterSharding,
           objectIO: ObjectIO,
           database: NitriteDatabase,
           notificationActorRef: ActorRef[ShardingEnvelope[NotificationCommand]])
          (implicit settings: Settings): ActorRef[ShardingEnvelope[Command]] =
    sharding.init(Entity(TypeKey)(_ => ObjectOperationsBehavior(settings.enableNotification, objectIO, database, notificationActorRef)))

  private def sanitizeVersionId(bucket: Bucket, maybeVersionId: Option[String]): Option[String] =
    if (BucketVersioning.NotExists == bucket.version && maybeVersionId.isDefined) {
      // if version id provided and versioning doesn't exists then set some dummy value
      // so that it results in NoSuckKey
      Some(UUID.randomUUID().toString)
    } else maybeVersionId
}
