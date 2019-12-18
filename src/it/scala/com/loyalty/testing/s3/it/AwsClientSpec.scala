package com.loyalty.testing.s3.it

import com.loyalty.testing.s3.it.client.{AwsClient, S3Client}

class AwsClientSpec extends S3IntegrationSpec("aws") {

  override protected val s3Client: S3Client = AwsClient()
}
