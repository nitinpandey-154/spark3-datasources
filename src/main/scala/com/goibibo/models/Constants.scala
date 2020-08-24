package com.goibibo.models


object Constants {

  val defaultAwsRegion = "ap-south-1"

  val timestampFormats: Map[Int, String] = Map(23 -> "yyyy-MM-dd HH:mm:ss.SSS",
    21 -> "yyyy-MM-dd HH:mm:ss.S",
    22 -> "yyyy-MM-dd HH:mm:ss.SS",
    24 -> "yyyy-MM-dd HH:mm:ss.SSSS",
    25 -> "yyyy-MM-dd HH:mm:ss.SSSSS",
    26 -> "yyyy-MM-dd HH:mm:ss.SSSSSS").withDefaultValue("yyyy-MM-dd HH:mm:ss")
  val splittableDataTypes = Array("INTEGER", "LONG", "DATETIME", "TIMESTAMP", "DATE", "BIGINT", "SMALLINT", "INT")

}
