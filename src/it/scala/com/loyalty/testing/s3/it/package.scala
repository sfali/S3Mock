package com.loyalty.testing.s3

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration

package object it {

  trait S3Settings {
    val endPoint: Option[EndpointConfiguration]
  }

  trait AwsSettings {
    val region: String
    val credentialsProvider: AWSCredentialsProvider
    val s3Settings: S3Settings
  }

}
