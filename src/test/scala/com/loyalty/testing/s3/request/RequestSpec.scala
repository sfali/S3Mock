package com.loyalty.testing.s3.request

import com.loyalty.testing.s3.defaultRegion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

class RequestSpec extends AnyFlatSpec with Matchers {

  it should "create `CreateBucketConfiguration` with valid xml" in {
    val xml =
      """
        |<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        | <LocationConstraint>us-west-1</LocationConstraint>
        |</CreateBucketConfiguration>
      """.stripMargin

    val config = CreateBucketConfiguration(Some(xml))
    config.locationConstraint mustEqual "us-west-1"
  }

  it should "create `CreateBucketConfiguration` with no `locationConstraint` contains default region" in {
    val xml =
      """
        |<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        |</CreateBucketConfiguration>
      """.stripMargin

    val config = CreateBucketConfiguration(Some(xml))
    config.locationConstraint mustEqual defaultRegion
  }

  it should "create `CreateBucketConfiguration` without any xml" in {
    val config = CreateBucketConfiguration(None)
    config.locationConstraint mustEqual defaultRegion
  }

  it should "create `VersioningConfiguration` with valid xml" in {
    val xml =
      """
        |<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        |  <Status>Enabled</Status>
        |</VersioningConfiguration>
      """.stripMargin

    val maybeVersioningConfiguration = VersioningConfiguration(Some(xml))
    maybeVersioningConfiguration mustBe defined
    maybeVersioningConfiguration.get.bucketVersioning mustBe BucketVersioning.Enabled
  }

  it should "not create `VersioningConfiguration` when no xml provided" in {
    val maybeVersioningConfiguration = VersioningConfiguration(None)
    maybeVersioningConfiguration mustBe empty
  }

  it should "not create `VersioningConfiguration` when status provided is not valid" in {
    val xml =
      """
        |<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        |  <Status>Illegal</Status>
        |</VersioningConfiguration>
      """.stripMargin

    val maybeVersioningConfiguration = VersioningConfiguration(Some(xml))
    maybeVersioningConfiguration mustBe empty
  }
}
