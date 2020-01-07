package com.loyalty.testing.s3

import com.loyalty.testing.s3.repositories.model.UploadInfo
import enumeratum.{CirceEnum, Enum, EnumEntry}

import scala.collection.immutable
import scala.util.Try
import scala.xml.NodeSeq

package object request {

  case class CreateBucketConfiguration(locationConstraint: String = defaultRegion) {}

  object CreateBucketConfiguration {

    def apply(maybeXml: Option[String]): CreateBucketConfiguration = {
      if (maybeXml.isDefined && maybeXml.get.trim.nonEmpty) {
        val node = scala.xml.XML.loadString(maybeXml.get.trim)
        val locationConstraint = Option(node.head).map(_.text.trim).filter(_.nonEmpty).getOrElse(defaultRegion)
        CreateBucketConfiguration(locationConstraint)
      } else CreateBucketConfiguration()
    }
  }

  case class VersioningConfiguration(bucketVersioning: BucketVersioning)

  object VersioningConfiguration {
    def apply(maybeXml: Option[String]): Option[VersioningConfiguration] = {
      if (maybeXml.isDefined && maybeXml.get.trim.nonEmpty) {
        val node = scala.xml.XML.loadString(maybeXml.get.trim)
        val statusNode = node \ "Status"
        val maybeValue = BucketVersioning.fromNode(statusNode)
        maybeValue.map(v => VersioningConfiguration(v))
      } else None
    }
  }

  case class PartInfo(partNumber: Int, eTag: String)

  object PartInfo {
    def apply(partNumber: Int, eTag: String): PartInfo = new PartInfo(partNumber, eTag)

    def apply(node: NodeSeq): PartInfo = {
      val partNumber = (node \ "PartNumber").text.toInt
      val eTag = (node \ "ETag").text.drop(1).dropRight(1) // remove quotations
      PartInfo(partNumber, eTag)
    }

    def apply(uploadInfo: UploadInfo): PartInfo = PartInfo(uploadInfo.partNumber, uploadInfo.eTag)

  }

  case class CompleteMultipartUpload(parts: List[PartInfo])

  object CompleteMultipartUpload {
    def apply(maybeXml: Option[String]): Option[CompleteMultipartUpload] =
      if (maybeXml.getOrElse("").trim.nonEmpty) {
        val node = scala.xml.XML.loadString(maybeXml.get.trim)
        val children = node \ "Part"
        Some(CompleteMultipartUpload(children.map(PartInfo.apply).toList))
      } else None
  }

  case class ObjectIdentifier(key: String, versionId: Option[String])

  object ObjectIdentifier {
    def apply(key: String, versionId: Option[String] = None): ObjectIdentifier = new ObjectIdentifier(key, versionId)

    def apply(node: NodeSeq): ObjectIdentifier = {
      val key = (node \ "Key").text.trim
      val versionId =
        Try((node \ "VersionId").text.trim).toOption match {
          case Some(value) => if (value.isEmpty) None else Some(value)
          case None => None
        }
      ObjectIdentifier(key, versionId)
    }
  }

  case class Delete(objects: List[ObjectIdentifier], verbose: Boolean)

  object Delete {
    def apply(objects: List[ObjectIdentifier], verbose: Boolean = true): Delete = new Delete(objects, verbose)

    def apply(maybeXml: Option[String]): Option[Delete] =
      if (maybeXml.getOrElse("").trim.nonEmpty) {
        val node = scala.xml.XML.loadString(maybeXml.get.trim)
        val objects = (node \ "Object").map(ObjectIdentifier(_)).toList
        val verbose =
          Try((node \ "Quite").text.trim).toOption match {
            case Some(value) => if (value.isEmpty) true else value.toBooleanOption.getOrElse(true)
            case None => true
          }
        Some(Delete(objects, verbose))
      } else None
  }

  sealed trait BucketVersioning extends EnumEntry

  object BucketVersioning extends Enum[BucketVersioning] with CirceEnum[BucketVersioning] {
    override def values: immutable.IndexedSeq[BucketVersioning] = findValues

    def fromNode(node: NodeSeq): Option[BucketVersioning] = Try(withName(node.text)).toOption

    case object Enabled extends BucketVersioning

    case object Suspended extends BucketVersioning

    case object NotExists extends BucketVersioning

  }

  case class ListBucketParams(maxKeys: Int = 1000,
                              maybePrefix: Option[String] = None,
                              maybeDelimiter: Option[String] = None)

}
