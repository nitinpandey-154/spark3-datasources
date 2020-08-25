

# spark3-datasources
Github Repo containing source code for spark3 datasources:

 - Optimized Mysql JDBC Reader
 - Amazon Redshift

## Requirements  
 - Spark 3

## Datasources

 - **gojdbc**: Optimized JDBC reader for MySQL built on top of [spark's JDBC reader](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html).
	
	<u>**Limitations of existing spark's JDBC Reader**</u>:  For batch pipelines not using [CDC](https://en.wikipedia.org/wiki/Change_data_capture#:~:text=In%20databases,%20change%20data%20capture,taken%20using%20the%20changed%20data.), it's important to keep a track of offset and use that offset next time when querying the source system. However, with spark, it wasn't feasible to use this functionality.
	
	* **Doesn't support filter pushdowns**: [Spark's JDBC Reader](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html) allows options such as `partitionColumn, lowerBound, upperBound` but it only uses this information to decide the partitioning.

		*Notice that `lowerBound` and `upperBound` are just used to decide the partition stride, not for filtering the rows in table. So all rows in the table will be partitioned and returned. This option applies only to reading.*
		
		So, effectively, the entire table would be returned as a dataframe which would be very large.
		
	* **Parallelism when running query option**: Spark does provide an option to pass a query as a parameter and inside query we can pass the offset. For example, in case of an e-commerce company, we can pass a query with last offset (`2020-07-12T11:23:10`) like this:
	`SELECT * FROM orders where modified_on > '2020-07-12T11:23:10`.
	
		The drawback here is that spark would not understand how to split this query into multiple executors and hence, uses a single partition to get all the data. 
	
	<u>**How gojdbc reader addresses this?**</u>

	 - A parameter `offset` can be passed along with `partitionColumn` and optionally `numPartitions` to generate the partitions. 
	 - It uses the provided `offset` (numeric or date/timestamp) and queries the source to get the minimum and maximum values of the `partitionColumn`. 
	 
		 For example - if the `offset` is `2020-07-12T11:23:10`. 
		 The minumum and maximum values returned can be `2020-07-12T11:23:24` and  `2020-07-12T41:31:32`.
		 
	- The `numPartitions` option creates multiple [partitions](https://github.com/apache/spark/blob/v3.0.0/core/src/main/scala/org/apache/spark/Partition.scala). The partition contains a `whereClause` which is used while reading from source. Refer [JDBCPartition](https://github.com/apache/spark/blob/v3.0.0/core/src/main/scala/org/apache/spark/rdd/JdbcRDD.scala) for more details.
	
	**Parameters:**
	* `url`: The JDBC URL (connection string) to establish connection to. Format - `jdbc:mysql://localhost/test?user=fred&password=secret`
	* `loadType`: Supports full or incremental. In full load, it will read the entire table into dataframe without any filtering. In incremental load, it accepts **partitionColumn**, **offset** and optionally **numPartitions** to decide filtering and partitioning.
	* `dbtable`: The table name in source database.
	* `offset`: Max value of the **partitionColumn** in the past run. The dataframe returned would have all the records having values greater than the offset for **partitionColumn** . Supports timestamp, date and numeric offsets. Example - `"2020-08-24 18:18:07.0"` for timestamp, `4345454` for numeric types and `"2020-08-24"` for date columns
	* `partitionColumn`: The column to be used for partitioning and filtering the dataframe. By filtering, we restrict the records that would be produced to dataframe. By partitioning, we create multiple partitions of dataframe (decided by **numPartitions**), and fire the same number of queries to mysql table.

	**Usage:**
	* **Incremental load with offset**
		```
		val df =spark
		.read
		.format("gojdbc")
		.option("url",url)
		.option("loadType","incremental")
		.option("dbtable","booking")
		.option("offset","2020-08-24 18:18:07.0")
		.option("partitionColumn","last_updated")
		.option("numPartitions",10)
		.load()
		```		
	* **Incremental load with offset without numPartitions**
		*Default value of 100 partitions would be used.*
		```
		val df =spark
		.read
		.format("gojdbc")
		.option("url",url)
		.option("loadType","incremental")
		.option("dbtable","booking")
		.option("offset","2020-08-24 18:18:07.0")
		.option("partitionColumn","last_updated")
		.load()
		```		
	* **Full load with partitionColumn and numPartitions**
		```
		val df =spark
		.read
		.format("gojdbc")
		.option("url",url)
		.option("loadType","full")
		.option("dbtable","booking")
		.option("partitionColumn","last_updated")
		.option("numPartitions",1204)
		.load()
		```		
	* **Full load with partitionColumn**
		```
		val df =spark
		.read
		.format("gojdbc")
		.option("url",url)
		.option("loadType","full")
		.option("dbtable","booking")
		.option("partitionColumn","last_updated")
		.load()
		```		
	* **Full load without partitionColumn**
		```
		val df =spark
		.read
		.format("gojdbc")
		.option("url",url)
		.option("loadType","full")
		.option("dbtable","booking")
		.load()
		```		
 
 - **redshift**: Optimized Redshift datasource with support for parquet. Currently, the default [spark-redshift datasource](https://github.com/databricks/spark-redshift) doesn't support unloading in parquet which affects the performance.
