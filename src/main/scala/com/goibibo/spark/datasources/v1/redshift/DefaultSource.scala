package com.goibibo.spark.datasources.v1.redshift

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

class DefaultSource extends
  RelationProvider with DataSourceRegister with Logging {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = ???

  //  {
  //    val url: String = parameters("url")
  //    val tableName: String = parameters("dbtable")
  //    val tempDir: String = parameters("tempdir")
  //    val query: String = parameters("query")
  //    val user: String = parameters.getOrElse("user", "")
  //    val password: String = parameters.getOrElse("password", "")
  //
  //
  //
  //  }

  override def shortName(): String = "goredshift"
}
