package org.apache.spark.sql.execution.datasources.jdbc

import org.apache.spark.Partition
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{Metadata, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, SQLContext, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

object JDBCAccess {
  def createRelation(sqlContext: SQLContext,
                     parameters: Map[String, String],
                     parts: Array[Partition]): BaseRelation = {
    val jdbcOptions = new JDBCOptions(parameters)
    val resolver = sqlContext.conf.resolver
    val timeZoneId = sqlContext.conf.sessionLocalTimeZone
    val schema = JDBCRelation.getSchema(resolver, jdbcOptions)
    val parts: Array[Partition] = JDBCRelation.columnPartition(schema, resolver, timeZoneId, jdbcOptions)
    JDBCRelation(schema, parts, jdbcOptions)(sqlContext.sparkSession)
  }

  def AnalysisException(exception: String): AnalysisException = {
    new AnalysisException(exception)
  }

  def getBaseRelation(parts: Array[Partition], options: JDBCOptions,
                      sourceDetails: Map[String, Any])(sparkSession: SparkSession): BaseRelation = {
    val schema = JDBCRelation.getSchema(sparkSession.sessionState.conf.resolver, options)
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    /*
        Adding metadata to dataframe schema
     */
    val newSchema = StructType(schema.map(structFieldObject => {
      val jsonString = Serialization.write(sourceDetails)
      val metadata = Metadata.fromJson(jsonString)
      StructField(structFieldObject.name, structFieldObject.dataType, structFieldObject.nullable, metadata)
    }))
    val relation = JDBCRelation(newSchema, parts, options)(sparkSession)
    relation
  }

}
