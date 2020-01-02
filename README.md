# S3Mock

## Introduction

`S3Mock` (available under Apache license V2) is an implementation `AWS S3 API` for local development and testing 
purpose.

### Implemented operations:

Following operations have been implemented:

1. [CreateBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html)
1. [PutBucketVersioning](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html)
1. [PutBucketNotificationConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketNotificationConfiguration.html)
1. [ListObjectsV2](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)
1. [PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)
1. [GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)
1. [HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)
1. [DeleteObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html)
1. [CopyObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html)
1. [CreateMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html)
1. [UploadPart](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html)
1. [UploadPartCopy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html)
1. [CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)

## Setup

`S3Mock` can be integrated to send `S3` notifications to either of `SQS` or `SNS` 