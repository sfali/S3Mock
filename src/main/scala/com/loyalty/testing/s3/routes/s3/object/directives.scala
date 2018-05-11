package com.loyalty.testing.s3.routes.s3.`object`

import java.net.URI
import java.nio.file.Paths

import akka.http.scaladsl.model.headers.ByteRange.{FromOffset, Slice, Suffix}
import akka.http.scaladsl.model.headers.{ByteRange, ModeledCustomHeader, ModeledCustomHeaderCompanion}

import scala.util.Try

object directives {

  final class `x-amz-copy-source`(source: String) extends ModeledCustomHeader[`x-amz-copy-source`] {
    private var _bucketName: String = _
    private var _key: String = _
    private var _maybeVersionId: Option[String] = _

    def bucketName: String = _bucketName

    def key: String = _key

    def maybeVersionId: Option[String] = _maybeVersionId

    override def companion: ModeledCustomHeaderCompanion[`x-amz-copy-source`] = `x-amz-copy-source`

    override def value(): String = source

    override def renderInRequests(): Boolean = true

    override def renderInResponses(): Boolean = false
  }

  object `x-amz-copy-source` extends ModeledCustomHeaderCompanion[`x-amz-copy-source`] {
    override val name = "x-amz-copy-source"

    override def parse(value: String): Try[`x-amz-copy-source`] =
      Try {
        val s = if (value.startsWith("/")) value else s"/$value"
        val uri = URI.create(s"file://$s")
        val path = Paths.get(uri.getPath)
        val bucketName = path.getName(0).toString
        val key = "/" + path.subpath(1, path.getNameCount).toString.replaceAll("\\\\", "/")
        val maybeVersionId = parseQueryString(Option(uri.getQuery))
        val source: `x-amz-copy-source` = new `x-amz-copy-source`(value)
        source._bucketName = bucketName
        source._key = key
        source._maybeVersionId = maybeVersionId
        source
      }
  }

  final class `x-amz-copy-source-range`(val range: ByteRange) extends ModeledCustomHeader[`x-amz-copy-source-range`] {
    override def companion: ModeledCustomHeaderCompanion[`x-amz-copy-source-range`] = `x-amz-copy-source-range`

    override def value(): String =
      range match {
        case Slice(first, last) => s"bytes=$first-$last"
        case FromOffset(offset) => s"bytes=$offset"
        case Suffix(length) => s"bytes=-$length"
      }

    override def renderInRequests(): Boolean = true

    override def renderInResponses(): Boolean = false

  }

  object `x-amz-copy-source-range` extends ModeledCustomHeaderCompanion[`x-amz-copy-source-range`] {
    override def name: String = "x-amz-copy-source-range"

    override def parse(value: String): Try[`x-amz-copy-source-range`] = {
      Try {
        val arr = value.drop(6).split("-")
        ByteRange(arr.head.toLong, arr.last.toLong)
      }.map(r => new `x-amz-copy-source-range`(r))
    }
  }

  private def parseQueryString(maybeQuery: Option[String]) = {
    maybeQuery.flatMap {
      queryString =>
        val versionIdQuery = queryString.split("&").find(_.contains("versionId"))
        parseVersionIdQuery(versionIdQuery)
    }
  }

  private def parseVersionIdQuery(maybeQuery: Option[String]) =
    maybeQuery.flatMap(query => Some(query.split("=").last))

}
