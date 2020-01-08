# S3Mock

## Introduction

`S3Mock` (available under Apache license V2) is an implementation `AWS S3 API` for local development and testing 
purpose.

### Implemented operations:

Following operations have been implemented:

1. [CreateBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html)
1. [HeadBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html)
1. [DeleteBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html)
1. [PutBucketVersioning](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html)
1. [PutBucketNotificationConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketNotificationConfiguration.html)
1. [ListObjectsV2](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)
1. [PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)
1. [GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)
1. [HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)
1. [DeleteObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html)
1. [DeleteObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html)
1. [CopyObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html)
1. [CreateMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html)
1. [UploadPart](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html)
1. [UploadPartCopy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html)
1. [CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)

## Setup

`S3Mock` uses persistent storage to store data, for persistence storage `S3Mock` uses [Nitrite databse](https://www.dizitart.org/nitrite-database.html) internally.
Upon first time initialization the database and necessary directory structures will be created, by default `.s3mock` 
directory will be used in user home directory for storage, this can be configured by using `DATA_DIRECTORY` environment 
variable. Following is the directory structure within the `DATA_DIRECTORY`:

* _data_ (contains actual data)
* _uploads_ (contains parts related to multi-part uploads)
* _uploads-staging_ (contains intimidatory parts for multi-part uploads)
* _s3mock.db_ (database file)

**NOTE:** Do not remove any file from the `DATA_DIRECTORY`, application is not tested for such behavior.

`S3Mock` supports versioning subresource and can be integrated to send `S3` notifications to `SQS` or `SNS`. By default 
notification integration is disabled and it can be enabled by using `ENABLE_NOTIFICATION` environment variable. In order 
to integrate with mock `SQS` and/or `SNS`, environment variables `SQS_END_POINT` and `SNS_END_POINT` can be configured. 
It is possible to integrate with _real_ `SQS` and `SNS`, set environment variable `AWS_CREDENTIAL_PROVIDER` to value 
`default` and make sure to provide proper environment variable(s) for the `AWS` provider chain, please consult `AWS` 
documentation for how to setup proper provider chain.

In real world scenario bucket(s), required to run application(s), to be pre-created, in order to support that `S3Mock` 
can be configured to create bucket(s) at the start up, in order to achieve this `S3Mock` following _json_ format for 
initialization:

```json
{
  "initialBuckets":[
    {
      "bucketName":"local-bucket-1",
      "enableVersioning":true
    },
    {
      "bucketName":"local-bucket-2",
      "enableVersioning":false
    }
  ],
  "notifications":[
    {
      "name":"53dba44f-9326-46b1-9b46-37efc5893a79",
      "notificationType":"ObjectCreated",
      "operationType":  "*",
      "destinationType":"Sqs",
      "destinationName":"local-queue-1",
      "bucketName":"local-bucket-1"
    },
    {
      "name":"ec6bb0f5-4934-43e4-b427-88fb9e591869",
      "notificationType":"ObjectCreated",
      "operationType":  "*",
      "destinationType":"Sns",
      "destinationName":"local-sns-1",
      "bucketName":"local-bucket-2"
    }
  ]
}
```

`S3Mock` supports following notification types and operations within each types:

* ObjectCreated (Put, Post, Copy, CompleteMultipartUpload, *)
* ObjectRemoved (Delete, DeleteMarkerCreated, *)

**NOTE:** Use _*_ to enable all operations for that type.

Initial data file can be configured by setting environment variable `INITIAL_DATA_FILE` to path to the file.

## Running application in docker environment 

**Build and create docker image:**

```shell script
./scripts/publish-local.sh
```

**Run Docker compose**

```shell script
docker-compose up
```
