package com.goibibo.spark.datasources.v1.gojdbc

import java.sql.Connection
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}

import com.goibibo.models.ConnectionDef
import com.goibibo.utils.{Helper, JDBCUtils, MysqlUtils, SparkUtils}
import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCAccess, JDBCOptions, JDBCPartition, JdbcRelationProvider}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

class DefaultSource extends JdbcRelationProvider with CreatableRelationProvider
  with RelationProvider with DataSourceRegister with Logging {

  override def shortName(): String = "gojdbc"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {

    val url: String = parameters("url")
    val tableName: String = parameters.getOrElse("dbtable",
      parameters.getOrElse("path", throw new Exception("Please provide the table name as dbtable or " +
        "inside the load(table_name) method call..")))
    val loadType: String = parameters.getOrElse("loadType", "incremental").toLowerCase
    val offset: String = parameters.getOrElse("offset", "")
    val column: String = parameters.getOrElse("column", "")
    val numPartitions: Int = parameters.getOrElse("numPartitions", "100").toInt

    val spark = SparkUtils.getActiveSparkSession
    val sparkApplicationId = SparkUtils.getSparkApplicationId
    val zone = ZoneId.systemDefault
    val zoneOffSet = zone.getRules.getOffset(LocalDateTime.now())
    var partitions = new ArrayBuffer[Partition]()

    val params: Map[String, String] = Map(
      "url" -> url,
      "tableName" -> tableName,
      "loadType" -> loadType,
      "offset" -> offset,
      "column" -> column,
      "numPartitions" -> numPartitions.toString
    )

    val jdbcPartitions: ArrayBuffer[Partition] = {
      for {
        connectionDef: ConnectionDef <- JDBCUtils.getConnectionDef(url)
        jdbcConnection <- MysqlUtils.createConnection(connectionDef)
        tableExists <- MysqlUtils.isTablePresent(connectionDef.databasename, tableName)(jdbcConnection)
        validation <- if (tableExists) Success("Table Present") else Failure(throw new Exception(
          s"Could not find table $tableName in the database ${connectionDef.databasename}"))

        partitions: ArrayBuffer[Partition] <-
          if (column != "") {
            generatePartitions(
              connectionDef.databasename, tableName, column, numPartitions: Int,
              offset, loadType, numPartitions)(jdbcConnection)
          }
          else {
            val partitions = new ArrayBuffer[Partition]()
            partitions += JDBCPartition("true", 0)
            Success(partitions)
          }
        _ <- Try(jdbcConnection.close())

      } yield partitions
    }
    match {
      case Success(p) => logInfo(s"Partitions - $p"); p
      case Failure(t) => t.printStackTrace(); throw new Exception("Load failed..");

    }


    val options: Map[String, String] = parameters ++ Map(
      "url" -> url,
      "dbtable" -> tableName,
      "partitionBy" -> column
    ) - "user" - "password" - "offset" - "loadType" - "restrictions" - "splittable" - "column"

    val jdbcOptions = new JDBCOptions(options)

    val sourceDetails: Map[String, Any] = Map(
      "tableName" -> tableName,
      "loadType" -> loadType,
      "sparkApplicationId" -> sparkApplicationId,
      "partitionColumn" -> column
    )

    JDBCAccess.getBaseRelation(jdbcPartitions.toArray, jdbcOptions, sourceDetails)(spark)
  }

  def getColumnRange(tableName: String, column: String, offset: String, loadType: String, columnType: String)(implicit connection: Connection): Try[(String, String)] = {

    if (loadType == "full") {
      MysqlUtils.getColumnRange(tableName, column, None)
    }
    else {
      // Incremental Load
      val filter =
        if (Helper.isNumberType(columnType)) {
          s" where $column > ${offset.toLong} "
        }
        else { // Timestamp type
          val _offset = for {
            timestampOffsetWithZerosPadded: String <- Helper.formatTimestamp(offset)
            timestampOffsetWithZerosWithoutTCharacter: String <- Helper.getTimestampFormat(timestampOffsetWithZerosPadded)
            timestampFormat: DateTimeFormatter <- Helper.getDateTimeFormatter(timestampOffsetWithZerosWithoutTCharacter)
            offsetTimestamp: LocalDateTime <- Try(
              LocalDateTime.parse(
                timestampOffsetWithZerosPadded, timestampFormat
              )
            )
          } yield offsetTimestamp
          s""" where $column > "${_offset.get}" """
        }
      MysqlUtils.getColumnRange(tableName, column, Some(filter))
    }
  }


  def generatePartitions(
                          databaseName: String, tableName: String, incrementalColumnName: String, partitionCount: Int,
                          offset: String, loadType: String, numPartitions: Int)(implicit connection: Connection): Try[ArrayBuffer[Partition]] = {

    val partitions = for {
      mysqlColumnNamesWithDatatypes <- MysqlUtils.getColumnsAndTypes(tableName)(connection)

      columnDataType: String <- Try {
        mysqlColumnNamesWithDatatypes(incrementalColumnName)
      }

      minMaxRangeForIncrementalColumn: (String, String) <- getColumnRange(tableName, incrementalColumnName, offset, loadType,
        columnDataType)(connection)

      lowerBoundString: String <- Try(minMaxRangeForIncrementalColumn._1)
      upperBoundString: String <- Try(minMaxRangeForIncrementalColumn._2)

      partitions: ArrayBuffer[Partition] <- generatePartitionsFromUpperAndLowerBounds(
        lowerBoundString, upperBoundString, databaseName, tableName, incrementalColumnName, columnDataType, numPartitions)(connection)

    } yield partitions
    partitions

  }

  def generatePartitionsFromUpperAndLowerBounds(lowerBoundString: String, upperBoundString: String,
                                                databaseName: String, tableName: String, column: String,
                                                columnType: String, partitionCount: Int)
                                               (implicit connection: Connection): Try[ArrayBuffer[Partition]] = {

    val partitions = new ArrayBuffer[Partition]()
    if (lowerBoundString == null) {
      logWarning("No new data found..")
      partitions += JDBCPartition("false", 0)
      Success(partitions)
    }
    else if (Helper.isNumberType(columnType)) {

      for {
        columnMin: Long <- Try(lowerBoundString.toLong)
        columnMax: Long <- Try(upperBoundString.toLong)
        numPartitions: Int <-
          if (columnMin > columnMax) {
            Failure(throw JDBCAccess.AnalysisException(s"The lower bound $columnMin is greater " +
              s"than upper bound $columnMax for $column. Failed to generate partitions..."))
          }
          else if (columnMax == columnMin) {
            logWarning(s"SETTING THE NUMBER OF PARTITIONS TO 1 SINCE MIN AND MAX ARE SAME " +
              s"- $columnMin")
            Success(1)
          }
          else if (((columnMax - columnMin) / partitionCount) < 1) {
            Success((columnMax - columnMin).toInt)
          }
          else {
            Success(partitionCount)
          }
        partitions: ArrayBuffer[Partition]
          <- Helper.generatePartitions(column, columnMin, columnMax, numPartitions)

      }
        yield partitions
    }
    else {
      // For datetime or timestamp types - Converts to YYYY-mm-DDTHH:MM:SS.ssssss format
      for {
        columnMin <- Helper.formatTimestamp(lowerBoundString)
        columnMax <- Helper.formatTimestamp(upperBoundString)
        timestampFormat: String <- Helper.getTimestampFormat(columnMax)
        datetimeFormatter: DateTimeFormatter <- Helper.getDateTimeFormatter(timestampFormat)
        columnMinTimestamp: LocalDateTime <- Try(LocalDateTime.parse(columnMin, datetimeFormatter))
        columnMaxTimestamp: LocalDateTime <- Try(LocalDateTime.parse(columnMax, datetimeFormatter))

        numPartitions: Int <-
          if (columnMaxTimestamp.isEqual(columnMinTimestamp)) {
            println(s"WARNING - SETTING THE NUMBER OF PARTITIONS TO 1 SINCE MIN AND MAX ARE SAME " +
              s"- $columnMaxTimestamp")
            Success(1)
          }
          else if (columnMaxTimestamp.isBefore(columnMinTimestamp)) {
            Failure(throw JDBCAccess.AnalysisException(
              s"The max value fetched - $columnMaxTimestamp for the" +
                s" column $column is " +
                s"smaller than the min value $columnMinTimestamp. " +
                s"Unable to generate partitions..Exiting."))
          }
          else Success(partitionCount)

        zoneOffset <- Try(ZoneId.systemDefault.getRules.getOffset(LocalDateTime.now()))

        partitions: ArrayBuffer[Partition] <- Helper.generatePartitions(column, columnMinTimestamp,
          columnMaxTimestamp, datetimeFormatter, zoneOffset, numPartitions)

      } yield partitions
    }
  }
}
