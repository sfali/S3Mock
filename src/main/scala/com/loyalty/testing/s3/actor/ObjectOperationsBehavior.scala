package com.loyalty.testing.s3.actor

import java.util.UUID

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, StashBuffer}
import com.loyalty.testing.s3._
import com.loyalty.testing.s3.actor.model.{DeleteInfo, InternalError, InvalidAccess, InvalidPart, InvalidPartOrder, MultiPartUploadedInitiated, NoSuchKeyExists, NoSuchUpload, ObjectContent, ObjectInfo, PartUploaded}
import com.loyalty.testing.s3.actor.model.`object`._
import com.loyalty.testing.s3.repositories.collections.NoSuchId
import com.loyalty.testing.s3.repositories.model.{Bucket, ObjectKey, UploadInfo}
import com.loyalty.testing.s3.repositories.{NitriteDatabase, ObjectIO}
import com.loyalty.testing.s3.request.BucketVersioning
import com.loyalty.testing.s3.service._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class ObjectOperationsBehavior(context: ActorContext[Command],
                               buffer: StashBuffer[Command],
                               objectService: ObjectService)
  extends AbstractBehavior(context) {

  import ObjectOperationsBehavior._

  private implicit val ec: ExecutionContext = context.system.executionContext
  private var versionIndex = 0
  private var objects: List[ObjectKey] = Nil
  private var uploadInfo: Option[UploadInfo] = None
  private var uploadParts: Map[String, Set[UploadInfo]] = Map.empty
  private val objectId = UUID.fromString(context.self.path.name)
  context.setReceiveTimeout(5.minutes, Shutdown)
  context.self ! InitializeSnapshot

  override def onMessage(msg: Command): Behavior[Command] =
    msg match {
      case InitializeSnapshot =>
        context.pipeToSelf(objectService.getAllObjects(objectId)) {
          case Failure(NoSuchId(_)) => ObjectResult(Nil)
          case Failure(ex) =>
            context.log.error(s"unable to load snapshot: $objectId", ex)
            // TODO: reply properly
            Shutdown
          case Success(values) => ObjectResult(values)
        }
        Behaviors.same

      case ObjectResult(values) =>
        val tuples = values.map(ok => (ok.index, ok.versionId))
        context.log.info("Current values: {} for {}", tuples, objectId)
        versionIndex = tuples.lastOption.map(_._1).getOrElse(0)
        objects = values
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

      case PutObject(bucket, key, contentSource, replyTo) =>
        versionIndex = if (BucketVersioning.Enabled == bucket.version) versionIndex + 1 else versionIndex
        context.pipeToSelf(objectService.saveObject(bucket, key, objectId, versionIndex, contentSource)) {
          case Failure(ex) =>
            context.log.error(s"unable to save object: ${bucket.bucketName}/$key", ex)
            DatabaseError
          case Success(objectKey) =>
            // TODO: notify notification router
            ReplyToSender(ObjectInfo(objectKey), replyTo, Some(objectKey))
        }
        Behaviors.same

      case DatabaseError =>
        Behaviors.same

      case GetObject(bucket, _, maybeVersionId, maybeRange, replyTo) =>
        val maybeObjectKey = getObject(sanitizeVersionId(bucket, maybeVersionId))
        val command =
          maybeObjectKey match {
            case None => ReplyToSender(NoSuchKeyExists, replyTo)
            case Some(objectKey) =>
              objectKey.deleteMarker match {
                case None =>
                  Try(objectService.getObject(objectKey, maybeRange)) match {
                    case Success((updatedObjectKey, source)) =>
                      ReplyToSender(ObjectContent(updatedObjectKey, source), replyTo)
                    case Failure(ex) =>
                      context.log.error("unable to download object", ex)
                      DatabaseError // TODO: retry
                  }
                case Some(_) =>
                  ReplyToSender(ObjectInfo(objectKey.copy(eTag = "", contentMd5 = "", contentLength = 0,
                    deleteMarker = Some(true))), replyTo)
              }
          }
        context.self ! command
        Behaviors.same

      case DeleteObject(bucket, key, maybeVersionId, replyTo) =>
        val maybeObjectKey = getObject(sanitizeVersionId(bucket, maybeVersionId))
        context.pipeToSelf(objectService.deleteObject(maybeObjectKey)) {
          case Failure(_: NoSuchKeyException.type) =>
            context.log.warn("unable to delete, reason=NoSuchKey, bucket_name={}, key={}, version_id={}",
              bucket.bucketName, key, maybeVersionId.getOrElse("None"))
            ReplyToSender(NoSuchKeyExists, replyTo)
          case Failure(ex) =>
            context.log.error(
              s"""unable to delete object: bucket_name=${bucket.bucketName}, key=$key,
                 | version_id=${maybeVersionId.getOrElse("None")}""".stripMargin.replaceNewLine, ex)
            DatabaseError // TODO: retry
          case Success(objectKey) =>
            val deleteInfo = DeleteInfo(objectKey.deleteMarker.getOrElse(false), objectKey.version,
              objectKey.actualVersionId)
            ReplyToSender(deleteInfo, replyTo, Some(objectKey))
        }
        Behaviors.same

      case InitiateMultiPartUpload(bucket, key, replyTo) =>
        versionIndex = if (BucketVersioning.Enabled == bucket.version) versionIndex + 1 else versionIndex
        val uploadInfo = UploadInfo(
          bucketName = bucket.bucketName,
          key = key,
          version = bucket.version,
          versionIndex = versionIndex,
          uploadId = createUploadId(bucket.bucketName, bucket.version, key, versionIndex)
        )
        context.pipeToSelf(objectService.createUpload(uploadInfo)) {
          case Failure(ex) =>
            context.log.error(s"unable to initiated multi part upload: ${bucket.bucketName}/$key", ex)
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
        context.self ! ReplyToSender(ObjectInfo(objectKey), replyTo, Some(objectKey))
        Behaviors.same

      case GetObjectMeta(replyTo) =>
        val maybeObjectKey = objects.find(_.id == objectId)
        val event = maybeObjectKey.map(ObjectInfo.apply).getOrElse(NoSuchKeyExists)
        context.self ! ReplyToSender(event, replyTo)
        Behaviors.same

      case ReplyToSender(reply, replyTo, maybeObjectMeta) =>
        objects =
          maybeObjectMeta match {
            case Some(objectKey) =>
              objectKey.version match {
                case BucketVersioning.Enabled => objects :+ objectKey
                case _ =>
                  val _objs = objects.filterNot(_.id == objectKey.id)
                  (_objs :+ objectKey).sortBy(_.index)
              }
            case None => objects
          }
        replyTo ! reply
        Behaviors.same

      case Shutdown => Behaviors.stopped

      case other =>
        context.log.warn("unhandled message: {}", other)
        Behaviors.unhandled
    }

  private def getObject(maybeVersionId: Option[String]) =
    maybeVersionId match {
      case Some(versionId) => objects.filter(_.versionId == versionId).lastOption
      case None => objects.lastOption
    }

}

object ObjectOperationsBehavior {

  def apply(objectIO: ObjectIO,
            database: NitriteDatabase): Behavior[Command] =
    Behaviors.setup[Command] { context =>
      Behaviors.withStash[Command](1000) { buffer =>
        new ObjectOperationsBehavior(context, buffer, ObjectService(objectIO, database))
      }
    }

  private def sanitizeVersionId(bucket: Bucket, maybeVersionId: Option[String]): Option[String] =
    if (BucketVersioning.NotExists == bucket.version && maybeVersionId.isDefined) {
      // if version id provided and versioning doesn't exists then set some dummy value
      // so that it results in NoSuckKey
      Some(UUID.randomUUID().toString)
    } else maybeVersionId
}
