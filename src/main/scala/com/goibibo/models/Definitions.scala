package com.goibibo.models

case class ConnectionDef(
                              drivername: String,
                              standard: String, //JDBC OR ODBC
                              dialect: String, // mysql, postgresql
                              hostname: String,
                              port: Int,
                              databasename: String,
                              username: String,
                              password: String
                            ) {
  def getConnectionUrl: String = {
    standard + ":" + dialect + "://" + hostname + ":" + port + "/" + databasename + "?user=" + username + "&password=" + password
  }

}

case class Field(fieldName: String, fieldDataType: String, precision: Int, scale: Int,
                 isNullable: Option[Boolean] = None, autoIncrement: Option[Boolean] = None)

case class Column(name: String, dataType: String, value: String)