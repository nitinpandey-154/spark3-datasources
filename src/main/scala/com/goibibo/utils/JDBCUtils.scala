package com.goibibo.utils

import scala.util.Try
import com.goibibo.models.ConnectionDef

object JDBCUtils {
  def getConnectionDef(url: String): Try[ConnectionDef] = Try {
    ConnectionDef(
      drivername = "com.mysql.jdbc.Driver",
      standard = url.split(":").head,
      dialect = url.split(":")(1),
      hostname = url.split("/")(2).split(":")(0),
      port = url.split("/")(2).split(":")(1).toInt,
      databasename = url.split("/")(3).split("\\?")(0),
      username = url.split("\\?")(1).split("user=")(1).split("&")(0),
      password = url.split("\\?")(1).split("password=")(1).split("&")(0)
    )
  }
}
