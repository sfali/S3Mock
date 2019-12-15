package com.loyalty.testing.s3

import com.loyalty.testing.s3.repositories.NitriteDatabase
import com.loyalty.testing.s3.request.ListBucketParams
import com.loyalty.testing.s3.response.BucketContent
import org.slf4j.Logger

package object actor {

  def listBucket(bucketName: String,
                 params: ListBucketParams)
                (database: NitriteDatabase,
                 log: Logger): List[BucketContent] = {
    val maybePrefix = params.maybePrefix.filterNot(_ == "")
    val maybeDelimiter = params.maybeDelimiter.filterNot(_ == "")
    val objects = database.getAllObjects(bucketName, maybePrefix)
    val bucketContents =
      if (maybeDelimiter.isDefined) {
        val delimiter = maybeDelimiter.get
        objects
          .flatMap {
            objectKey =>
              val key = objectKey.key
              val index = maybePrefix.map(prefix => key.indexOf(prefix) + prefix.length).getOrElse(0)
              if (index <= -1) BucketContent(objectKey) :: Nil
              else {
                val otherIndex = key.indexOf(delimiter, index) + 1
                if (otherIndex <= -1) {
                  log.warn("Unable to find delimiter '{}', that must be due to the fact that delimiter is not '/'",
                    delimiter)
                  BucketContent(objectKey) :: Nil
                } else if (otherIndex == 0) BucketContent(objectKey) :: Nil
                else {
                  BucketContent(expand = true, key.substring(0, index), 0, "") ::
                    BucketContent(expand = false, key.substring(0, otherIndex), 0, "") :: Nil
                }
              }
          }
      } else objects.map(BucketContent(_))
    bucketContents.toSet.toList.take(params.maxKeys)
  }
}
