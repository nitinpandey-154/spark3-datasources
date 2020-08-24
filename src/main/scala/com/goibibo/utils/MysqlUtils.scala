package com.goibibo.utils

import java.sql._

import com.goibibo.models.Field
import com.goibibo.models.ConnectionDef

import scala.util.{Failure, Success, Try}

object MysqlUtils {


  /**
   * Returns true or false depending upon the i passed
   *
   * @param i Integer value
   * @return Boolean Value - True or False
   */
  def intToBool(i: Int): Boolean = if (i == 0) false else true

  /**
   * Returns a Connection wrapped inside scala.util.Try
   *
   * @param connectionSpecs Value of type ConnectionSpecs
   * @return Success(Connection) or Failure(t:Throwable)
   */
  def createConnection(connectionSpecs: ConnectionDef): Try[Connection] = {
    Class.forName(connectionSpecs.drivername)
    Try(
      DriverManager.getConnection(
        connectionSpecs.getConnectionUrl,
        connectionSpecs.username,
        connectionSpecs.password)
    ) recoverWith {
      case t: Throwable => t.printStackTrace(); Failure(t)
    }
  }

  def createConnection(connectionUrl: String): Try[Connection] = {
    Class.forName("com.mysql.jdbc.Driver")
    Try(
      DriverManager.getConnection(connectionUrl)
    ) recoverWith {
      case t: Throwable => t.printStackTrace(); Failure(t)
    }
  }

  /**
   * Create a mysql table
   *
   * @param createTableStatement DDL query
   * @param connection           Implicit type of java.sql.Connection
   * @return None; throws Exception if fails
   */
  def createTable(createTableStatement: String)(implicit connection: Connection): Try[Int] = Try {
    val stmt = connection.createStatement
    stmt.executeUpdate(createTableStatement)
  }


  /**
   * Drop a mysql table
   *
   * @param tableName  String
   * @param connection Implicit type of java.sql.Connection
   * @return None; throws Exception if fails
   */
  def dropTable(databaseName: String, tableName: String)(implicit connection: Connection): Try[Int] = Try {

    val stmt = connection.createStatement
    stmt.executeUpdate(s"DROP TABLE IF EXISTS $tableName")
  }


  /**
   * Returns a boolean flag depending upon if the table exists in Mysql
   *
   * @param databaseName Name of the Mysql Database
   * @param tableName    Mysql Table Name
   * @param connection   Implicit type of java.sql.Connection
   * @return true or false
   */
  def isTablePresent(databaseName: String, tableName: String)(implicit connection: Connection): Try[Boolean] = Try {

    val tableExists = connection.getMetaData.getTables(
      databaseName,
      null,
      tableName,
      null).next
    tableExists
  }

  /**
   * Returns a boolean flag depending upon if the table exists in Mysql
   *
   * @param connectionSpecs Value of type ConnectionSpecs
   * @param tableName       Mysql Table Name
   * @return true or false
   */
  def isTablePresent(connectionSpecs: ConnectionDef, tableName: String): Try[Boolean] = {

    createConnection(connectionSpecs) match {
      case Failure(t) => t.printStackTrace(); throw new Exception("Could not create a mysql connection...")
      case Success(conn) =>
        val tableExists = isTablePresent(connectionSpecs.databasename, tableName)(conn)
        conn.close()
        tableExists
    }

  }

  /**
   * Returns a list of primary keys for a given mysql table
   *
   * @param databaseName Name of the Mysql Database
   * @param tableName    Mysql Table Name
   * @param connection   Implicit type of java.sql.Connection
   * @return Set[String] containing the primary keys
   */
  def getPrimaryKeys(databaseName: String, tableName: String)(implicit connection: Connection): Try[Set[String]] = Try {

    var primaryKeys = scala.collection.immutable.Set[String]()

    val keys = connection.getMetaData.getPrimaryKeys(databaseName, null, tableName)
    while (keys.next()) {
      val columnName = keys.getString(4)
      primaryKeys = primaryKeys + columnName
    }
    primaryKeys
  }

  /**
   * Returns a list of primary keys for a given mysql table
   *
   * @param connectionSpecs Value of type ConnectionSpecs
   * @param tableName       Mysql Table Name
   * @return Set[String] containing the primary keys
   */
  def getPrimaryKeys(connectionSpecs: ConnectionDef, tableName: String): Try[Set[String]] = {
    createConnection(connectionSpecs) match {
      case Failure(t) => t.printStackTrace(); throw new Exception(t)
      case Success(conn) => {
        val primaryKeys = getPrimaryKeys(connectionSpecs.databasename, tableName)(conn)
        conn.close()
        primaryKeys
      }
    }

  }

  /**
   * Returns a set of indexes for a given mysql table
   *
   * @param databaseName Name of the Mysql Database
   * @param tableName    Mysql Table Name
   * @param connection   Implicit type of java.sql.Connection
   * @return Set[String] containing the indexed columns
   */
  def getIndexes(databaseName: String, tableName: String)(implicit connection: Connection): Try[Set[String]] = Try {
    var indexes = scala.collection.immutable.Set[String]()
    val keys = connection.getMetaData.getIndexInfo(
      databaseName,
      null,
      tableName,
      false,
      false)
    while (keys.next()) {
      val columnName = keys.getString(9)
      indexes = indexes + columnName
    }
    indexes
  }


  /**
   * Returns a set of indexes for a given mysql table
   *
   * @param connectionSpecs Value of type ConnectionSpecs
   * @param tableName       Mysql Table Name
   * @return Set[String] containing the indexed columns
   */
  def getIndexes(connectionSpecs: ConnectionDef, tableName: String): Try[Set[String]] = {
    createConnection(connectionSpecs) match {
      case Failure(t) => t.printStackTrace(); throw new Exception(t)
      case Success(conn) => {
        val indexes = getIndexes(connectionSpecs.databasename, tableName)(conn)
        conn.close()
        indexes
      }
    }

  }


  /**
   * Returns the record count for a given query
   *
   * @param query      Query to get the record count
   * @param connection Implicit type of java.sql.Connection
   * @return Long value representing record count
   */
  def getRecordCount(query: String)(implicit connection: Connection): Try[Long] = Try {
    val statement = connection.createStatement()
    val rs = statement.executeQuery(query)
    rs.next()
    rs.getString(1).toLong
  }


  /**
   * * Returns the record count for a given query
   *
   * @param connectionSpecs Value of type ConnectionSpecs
   * @param query           Query to get the record count
   * @return Long value representing record count
   */
  def getRecordCount(connectionSpecs: ConnectionDef, query: String): Try[Long] = {
    createConnection(connectionSpecs) match {
      case Failure(t) => t.printStackTrace(); throw new Exception(t)
      case Success(conn) => {
        val recordCount = getRecordCount(query)(conn)
        conn.close()
        recordCount
      }
    }
  }

  /**
   * * Returns the schema of the table having metadata in type of Seq[Field]
   *
   * @param tableName  Mysql Table Name
   * @param connection Implicit type of java.sql.Connection
   * @return Seq[Field]
   */
  def getSchema(tableName: String)(implicit connection: Connection): Try[Seq[Field]] = Try {

    val query = s"SELECT * FROM $tableName limit 1"
    val statement = connection.createStatement
    val rs: ResultSet = statement.executeQuery(query)
    val metadata = rs.getMetaData
    val schema = (1 to metadata.getColumnCount).map {
      index =>
        Field(metadata.getColumnName(index),
          metadata.getColumnTypeName(index),
          metadata.getPrecision(index),
          metadata.getScale(index),
          Some(intToBool(metadata.isNullable(index))),
          Some(metadata.isAutoIncrement(index))
        )
    }
    schema
  }

  /**
   * * Returns the schema of the table having metadata in type of Seq[Field]
   *
   * @param connectionSpecs Value of type ConnectionSpecs
   * @param tableName       Mysql Table Name
   * @return Seq[Field]
   */
  def getSchema(connectionSpecs: ConnectionDef, tableName: String): Try[Seq[Field]] = {

    createConnection(connectionSpecs) match {
      case Failure(t) => t.printStackTrace(); throw new Exception(t)
      case Success(conn) => {
        val schema = getSchema(tableName)(conn)
        conn.close()
        schema
      }
    }

  }


  /**
   * * Returns the average record size of mysql table
   *
   * @param databaseName Mysql Database Name
   * @param tableName    Mysql Table Name
   * @param connection   Implicit type of java.sql.Connection
   * @return Average record size
   */
  def getMysqlAvgRowSize(databaseName: String, tableName: String)(implicit connection: Connection): Try[Long] = {
    Try {

      val query = s"SELECT avg_row_length FROM information_schema.tables WHERE table_schema = " +
        s"'$databaseName' AND table_name = '$tableName'"
      val result: ResultSet = connection.createStatement().executeQuery(query)
      val size = Try {
        result.next()
        result.getLong(1)
      } match {
        case Success(size) => size
        case Failure(e) =>
          println("Stack Trace: ", e.fillInStackTrace())
          5000
      }
      result.close()
      size
    }
  }

  /**
   * * Returns the average record size of mysql table
   *
   * @param connectionSpecs Value of type ConnectionSpecs
   * @param tableName       Mysql Table Name
   * @return Average record size
   */
  def getMysqlAvgRowSize(connectionSpecs: ConnectionDef, tableName: String): Try[Long] = {


    val query = s"SELECT avg_row_length FROM information_schema.tables WHERE table_schema = " +
      s"'${connectionSpecs.databasename}' AND table_name = '$tableName'"
    val connection = createConnection(connectionSpecs) match {
      case Failure(e) => e.printStackTrace()
        throw new Exception(e.getMessage)
      case Success(conn) => conn
    }
    val size: Try[Long] = getMysqlAvgRowSize(connectionSpecs.databasename, tableName)(connection)
    connection.close()
    size

  }

  /**
   * * Get the min and max of a column with the given filters
   *
   * @param tableName  Mysql Table Name
   * @param columnName Name of the column in mysql table
   * @param filters    Filter conditions applied while fetching min and max. Type - Option[String]
   * @param connection Implicit type of java.sql.Connection
   * @return Tuple containing the min and max of the column with applied filters (if any)
   */
  def getColumnRange(tableName: String, columnName: String, filters: Option[String])(
    implicit connection: Connection): Try[(String, String)] = {
    Try {

      val statement = connection.createStatement
      val query = if (filters.isDefined)
        s"SELECT min($columnName) as min_value, max($columnName) as max_value FROM $tableName ${filters.get}"
      else s"SELECT min($columnName) as min_value, max($columnName) as max_value FROM $tableName"
      val rs = statement.executeQuery(query)
      rs.next()
      val min = rs.getString("min_value")
      val max = rs.getString("max_value")
      rs.close()
      statement.close()
      (min, max)
    }
  }


  /**
   * * Get the min and max of a column with the given filters
   *
   * @param connectionSpecs Value of type ConnectionSpecs
   * @param tableName       Mysql Table Name
   * @param columnName      Name of the column in mysql table
   * @param filters         Filter conditions applied while fetching min and max. Type - Option[String]
   * @return Tuple containing the min and max of the column with applied filters (if any)
   */
  def getColumnRange(connectionSpecs: ConnectionDef, tableName: String,
                     columnName: String, filters: Option[String]): Try[(String, String)] = {
    val connection = createConnection(connectionSpecs) match {
      case Failure(e) => e.printStackTrace()
        throw new Exception(e.getMessage)
      case Success(conn) => conn
    }
    val columnRange = getColumnRange(tableName, columnName, filters)(connection)
    connection.close()
    columnRange
  }

  /**
   * * Get column name with types foor a given table
   *
   * @param tableName  Mysql Table Name
   * @param connection Implicit type of java.sql.Connection
   * @return Returns a map of columnName and columnDatatype
   */
  def getColumnsAndTypes(tableName: String)(implicit connection: Connection): Try[Map[String, String]] = Try {
    getSchema(tableName) match {
      case Success(schema) => schema.map(field => (field.fieldName, field.fieldDataType)).toMap
      case Failure(t) => t.printStackTrace(); throw new Exception(t)
    }
  }

  /**
   * * Get column name with types foor a given table
   *
   * @param connectionSpecs Value of type ConnectionSpecs
   * @param tableName       Mysql Table Name
   * @return Returns a map of columnName and columnDatatype
   */
  def getColumnsAndTypes(connectionSpecs: ConnectionDef, tableName: String): Try[Map[String, String]] = {
    createConnection(connectionSpecs) match {
      case Failure(t) => t.printStackTrace(); throw new Exception(t)
      case Success(conn) => {
        val columnWithTypes: Try[Map[String, String]] = getColumnsAndTypes(tableName)(conn)
        conn.close()
        columnWithTypes
      }

    }
  }


  /**
   * * Get column list of a mysql table
   *
   * @param tableName  Mysql Table Name
   * @param connection Implicit type of java.sql.Connection
   * @return Returns a sequence of columnNames
   */
  def getColumnList(tableName: String)(implicit connection: Connection): Try[Seq[String]] = Try {
    getSchema(tableName) match {
      case Success(schema) => schema.map(field => field.fieldName)
      case Failure(t) => t.printStackTrace(); throw new Exception(t)
    }


  }


  /**
   * * Get column list of a mysql table
   *
   * @param connectionSpecs Value of type ConnectionSpecs
   * @param tableName       Mysql Table Name
   * @return Returns a sequence of columnNames
   */
  def getColumnList(connectionSpecs: ConnectionDef, tableName: String): Try[Seq[String]] = {
    createConnection(connectionSpecs) match {
      case Failure(t) => t.printStackTrace(); throw new Exception(t)
      case Success(conn) => {
        val columns = getColumnList(tableName)(conn)
        conn.close()
        columns

      }
    }
  }

}

