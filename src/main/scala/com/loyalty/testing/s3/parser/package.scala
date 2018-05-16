package com.loyalty.testing.s3

import io.circe.syntax._
import io.circe.{Encoder, Printer}

package object parser {

  private val printer =
    Printer(
      preserveOrder = true,
      dropNullValues = true,
      indent = " ",
      lbraceRight = " ",
      rbraceLeft = " ",
      lbracketRight = System.lineSeparator(),
      rbracketLeft = System.lineSeparator(),
      lrbracketsEmpty = System.lineSeparator(),
      arrayCommaRight = System.lineSeparator(),
      objectCommaRight = " ",
      colonLeft = " ",
      colonRight = " "
    )

  def toJsonString[A](entity: A)(implicit encoder: Encoder[A]): String = printer.pretty(entity.asJson)

}
