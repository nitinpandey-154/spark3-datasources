package com.goibibo.utils

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import com.goibibo.models.Constants
import org.apache.spark.Partition
import org.apache.spark.sql.execution.datasources.jdbc.JDBCPartition

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object Helper {
  def caseClassToMap(cc: AnyRef): Map[String, Any] =
    cc.getClass.getDeclaredFields.foldLeft(Map.empty[String, Any]) { (a, f) =>
      f.setAccessible(true)
      a + (f.getName -> f.get(cc))
    }

  def isNumberType(columnType: String): Boolean = {
    if (columnType.toUpperCase.contains("INT") || columnType.toUpperCase.contains("LONG"))
      true
    else
      false
  }

  def formatTimestamp(ts: String): Try[String] = Try {
    if (ts.contains(".")) {
      val zerosToPad = 6 - ts.split("\\.")(1).length
      val zeros = "0" * zerosToPad
      ts + zeros
    }
    else
      ts + ".000000"
  }

  def getTimestampFormat(ts: String): Try[String] = Try {
    val timestampLength = ts.length
    var format = Constants.timestampFormats(timestampLength)

    if (ts.contains("T")) {
      format = format.replace(" ", "'T")
    }
    format
  }

  def getDateTimeFormatter(format: String): Try[DateTimeFormatter] = Try {
    DateTimeFormatter.ofPattern(format)
  }

  def generatePartitions(column: String, lowerBound: LocalDateTime, upperBound: LocalDateTime,
                         timestampFormat: DateTimeFormatter, zoneOffset: ZoneOffset, totalPartitions: Int): Try[ArrayBuffer[Partition]] = Try {

    val partitions = new ArrayBuffer[Partition]()
    val lowerBoundEpoch = lowerBound.toEpochSecond(zoneOffset)
    val upperBoundEpoch = upperBound.toEpochSecond(zoneOffset)

    val step = (upperBoundEpoch - lowerBoundEpoch) / totalPartitions

    val ranges: Seq[String] = (0 to totalPartitions.toInt).map(index =>
      lowerBound.plusSeconds(step * index).format(timestampFormat)
    )

    var index = 0
    while (index < totalPartitions) {
      val lower = ranges(index)
      val upper = if (index == totalPartitions - 1) upperBound else ranges(index + 1)
      val condition = if (index == totalPartitions - 1) "<=" else "<"
      partitions += JDBCPartition(
        s""" `$column`>="$lower" AND
           | `$column` $condition "$upper" """.stripMargin, index)
      index = index + 1
    }
    partitions

  }


  def generatePartitions(column: String, lowerBound: Long, upperBound: Long, totalPartitions: Int): Try[ArrayBuffer[Partition]] = Try {

    val partitions = new ArrayBuffer[Partition]()

    val step = (upperBound - lowerBound) / totalPartitions

    val ranges = (0 to totalPartitions.toInt).map(i => lowerBound + step * i)

    var index = 0
    while (index < totalPartitions) {
      val lower = ranges(index)
      val condition = if (index == totalPartitions - 1) "<=" else "<"
      val upper = if (index == totalPartitions - 1) upperBound else ranges(index + 1)
      partitions += JDBCPartition(
        s" `$column`>=" + lower + s" AND" +
          s" `$column`$condition" + upper, index)
      index = index + 1
    }
    partitions
  }


}
