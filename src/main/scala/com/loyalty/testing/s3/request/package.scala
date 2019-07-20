package com.loyalty.testing.s3

import com.loyalty.testing.s3.request.BucketVersioning.BucketVersioning

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

  case class UploadPart(partNumber: Int, eTag: String)

  object UploadPart {
    def apply(node: NodeSeq): UploadPart = {
      val partNumber = (node \ "PartNumber").text.toInt
      val eTag = (node \ "ETag").text
      UploadPart(partNumber, eTag)
    }
  }

  case class CompleteMultipartUpload(parts: List[UploadPart])

  object CompleteMultipartUpload {
    def apply(maybeXml: Option[String]): Option[CompleteMultipartUpload] =
      if (maybeXml.getOrElse("").trim.nonEmpty) {
        val node = scala.xml.XML.loadString(maybeXml.get.trim)
        val children = node \ "Part"
        Some(CompleteMultipartUpload(children.map(UploadPart.apply).toList))
      } else None
  }

  object BucketVersioning extends Enumeration {
    type BucketVersioning = Value
    val Enabled, Suspended = Value

    def fromNode(node: NodeSeq): Option[request.BucketVersioning.Value] = Try(withName(node.text)).toOption
  }

  case class ListBucketParams(maxKeys: Int,
                              maybePrefix: Option[String] = None,
                              maybeDelimiter: Option[String] = None)

}
