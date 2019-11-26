package com.loyalty.testing.s3.it

import com.loyalty.testing.s3.it.client.{AlpakkaClient, S3Client}

class AlpakkaClientSpec extends S3IntegrationSpec(rootPath, "alpakka") {

  override protected val s3Client: S3Client = AlpakkaClient()
}
