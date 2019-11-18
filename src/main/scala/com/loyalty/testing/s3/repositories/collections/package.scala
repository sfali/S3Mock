package com.loyalty.testing.s3.repositories

import java.time.OffsetDateTime

package object collections {

  case class CreateResponse(id: Long, lastModifiedTime: OffsetDateTime)

  case class InvalidInputException(message: String) extends Exception(message)

  case class DatabaseAccessException(message: String) extends Exception(message)

}
