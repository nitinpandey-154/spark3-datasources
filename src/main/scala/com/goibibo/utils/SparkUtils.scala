package com.goibibo.utils

import org.apache.spark.sql.functions.base64
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Try}

object SparkUtils {

  def getActiveSparkSession: SparkSession = SparkSession.builder().getOrCreate()

  def getSparkApplicationId: String = getActiveSparkSession.sparkContext.applicationId

  def writeDF(df: DataFrame, format: String, mode: String, targetPath: String, partitionBy: Option[Seq[String]] = None): Try[Unit] = {
    Try {
      if (partitionBy.isEmpty) {
        df.write.format(format).mode(mode).save(targetPath)
      }
      else
        df.write.format(format).mode(mode).partitionBy(partitionBy.get: _*).save(targetPath)
    } recoverWith { case t: Throwable => t.printStackTrace(); Failure(t) }

  }

  def convertBinaryTypesToBase64EncodedString(dataframe: DataFrame): Try[DataFrame] = {
    Try {

      val schema = dataframe.schema
      val binaryColumns = schema filter {
        _.dataType == BinaryType
      } map {
        _.name
      }
      var df = dataframe
      for (columnName <- binaryColumns) {
        var newDF = df.withColumn(
          s"${columnName}_base64_string", base64(df(columnName))).
          drop(columnName).
          withColumnRenamed(s"${columnName}_base64_string", columnName)
        df = newDF
      }
      df
    }
  }

  def getDataframeFromMysql(tableName: String, url: String, loadType: String, offset: String, restrictions: Boolean,
                            column: String, splittable: Boolean, numPartitions: Int, primaryKeys: Array[String]
                           ): DataFrame = {

    val spark = SparkUtils.getActiveSparkSession
    val df = spark.read.format("com.goibibo.datasources.spark.GoJDBC")
      .option("url", url)
      .option("loadType", loadType)
      .option("offset", offset)
      .option("restrictions", restrictions)
      .option("column", column)
      .option("splittable", splittable)
      .option("numPartitions", numPartitions)
      .load(tableName)

    if (loadType == "full") {
      df
    }
    else
      df.dropDuplicates(primaryKeys)

    /*
        Potential issue - When deduplicating new record with actual update might get lost
     */
  }
}

