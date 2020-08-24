name := "spark3-datasources"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.0.0" % "provided",
  "mysql" % "mysql-connector-java" % "5.1.38"
)
