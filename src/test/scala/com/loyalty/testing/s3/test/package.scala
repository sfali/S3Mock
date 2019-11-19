package com.loyalty.testing.s3

import com.loyalty.testing.s3.utils.StaticDateTimeProvider

package object test {

  implicit val dateTimeProvider: StaticDateTimeProvider = StaticDateTimeProvider()
}
