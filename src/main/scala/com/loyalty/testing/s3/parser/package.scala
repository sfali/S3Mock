package com.loyalty.testing.s3

import io.circe.syntax._
import io.circe.{Encoder, Printer}

package object parser {

  private val printer = Printer.noSpaces.copy(dropNullValues = true)

  def toJsonString[A](entity: A)(implicit encoder: Encoder[A]): String = printer.print(entity.asJson)

}
