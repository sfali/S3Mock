package com.loyalty.testing.s3.notification

case class NotificationData(bucketName: String,
                            key: String,
                            size: Long,
                            eTag: String,
                            maybeVersionId: Option[String] = None)
