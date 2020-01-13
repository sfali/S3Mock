package com.loyalty.testing.s3.repositories

import java.util.UUID

package object collections {

  /*case class CreateResponse(id: UUID, lastModifiedTime: OffsetDateTime)

  case class InvalidInputException(message: String) extends Exception(message)*/

  case class DatabaseAccessException(message: String) extends Exception(message)

  case class BucketAlreadyExistsException(bucketName: String) extends Exception(s"Bucket $bucketName already exists")

  case class BucketNotEmptyException(bucketName: String) extends Exception(s"Bucket $bucketName is not empty")

  case class NoSuchId(id: UUID) extends Exception(s"Specified id `$id` does not exists")

  case class NoSuchPart(id: UUID, uploadId: String, partNumber: Int) extends Exception(s"No such part: $id/$uploadId/$partNumber")

  case class NotMultiPartUpload(id: UUID) extends Exception(s"Specified object ($id) is not uploaded by multipart")

}
