package org.adilazh1

import org.apache.log4j.Level
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Row, SparkSession, hive}
import org.apache.spark.sql.types._
import org.mongodb.scala.bson._


/**
 * The objective of this exercise is load a json with information about Bicing, cleaning this file and
 * upload it in a hive table.
 * We will practice upload data to hive using spark.
 *
 */

object App  {

  def main(args: Array[String]): Unit = {

    //load file, path in the arguments. In more complex examples, is better using a properties file
    val path = args(0)

    import org.apache.log4j.Logger
    //create a Spark Session
    val spark = SparkSession.builder.appName("bicing").master("local[*]")
      .config("spark.sql.warehouse-dir","/user/hive/warehouse")
      .config("hive.exec.scratchdir","/tmp/tmpHive")
      .config("hive.metastore.uris","thrift://localhost:9083")
      .enableHiveSupport() // Hive
      .getOrCreate

    val cnt = spark.sparkContext
    val file = cnt.textFile(path)
    Logger.getRootLogger.setLevel(Level.ERROR)

    //We extract the array Document "stations" imbibed in data
    val doc_data = file.map(Document.apply(_).get("data").get.asDocument().getArray("stations"))

    //Extract every document in the array and split it in a Row format. Attention: use Row.fromSeq() return only Strings, define a function.
    val doc_stations = doc_data.map(f=>{
      val iter = f.iterator()
      var out = ""
      while(iter.hasNext) {
        val doc = iter.next().asDocument()
        out += s"${doc.get("station_id").asInt32().getValue};" +
               s"${doc.get("name").asString().getValue};" +
               s"${doc.get("physical_configuration").asString().getValue};" +
               s"${doc.get("lat").asDouble().getValue};" +
               s"${doc.get("lon").asDouble().getValue};" +
               s"${doc.get("altitude").asDouble().getValue};" +
               s"${doc.get("address").asString().getValue};" +
               s"${doc.get("post_code").asString().getValue};" +
               s"${doc.get("capacity").asInt32().getValue}" +
               s"\n"
      }
      out
    }  ).flatMap(_.split("\n")).map(_.split(";").toList).map(row(_))

    import spark.implicits._
    val schema = dfSchema()
    val df = spark.createDataFrame(doc_stations,schema)
    df.cache()

    /*
    Now the objective is create a hive table and save our dataframe
     */

    /*
    Fist, save table without use hive Context
    sudo -u hive hadoop fs -chmod 777 /tmp/hive/
    its give issues related to permisions, we will create a new folder /tmp/tmpHive and indicate spark to use it
    also we add resources folder to project with hive-site.xml file
    Using hiveContext we can avoid this issue
     */

    df.write.mode("overwrite").saveAsTable("bicing.bicing_1")
    //Note: file is save as parquet compreesed snappy!!

    //save as textfile
    //We need to create te table and specific stored as textfile
    spark.sqlContext.sql("CREATE TABLE IF NOT EXISTS bicing.bicing_2 (" +
      "station_id INT, name STRING, physical_configuration STRING, lat DOUBLE, lon DOUBLE, altitude DOUBLE, address STRING, post_code INT, capacity INT)" +
      "STORED AS TEXTFILE")
    df.createOrReplaceTempView("table")
    spark.sqlContext.sql("INSERT INTO bicing.bicing_2 (SELECT * FROM table)")

    //Now, we will use HiveContext to save tables.
    val hiveCnt = new HiveContext(cnt) //Note that hiveContext is deprecated, use SparkContext

    hiveCnt.sql("CREATE TABLE IF NOT EXISTS bicing.bicing_3 as select * from table")

  }

  def dfSchema(): StructType =
    StructType(
      List(
        StructField("station_id", IntegerType, false),
        StructField("name", StringType, false),
        StructField("physical_configuration", StringType, false),
        StructField("lat", DoubleType, false),
        StructField("lon", DoubleType, false),
        StructField("altitude", DoubleType, false),
        StructField("address", StringType, false),
        StructField("post_code", IntegerType, false),
        StructField("capacity", IntegerType, false)
      )
    )

  def row(line: List[String]): Row = Row(line(0).toInt, line(1).toString, line(2).toString, line(3).toDouble, line(4).toDouble
  , line(5).toDouble, line(6).toString, line(7).toInt, line(8).toInt)


}

/**
 * Conclusions:
 * 1. To interactive with hive, we need enableHiveSupport. Giving it  spark.sql.warehouse-dir; hive.exec.scratchdir and hive.metastore.uris
 * 2. add resources folder with hive-site.xml file. This is necessary only for maven projects
 * 3. From RDD to DF, we need create schema using StructType
 * 4. map functions work like one-to-one and flatMap functions work like one-to-many
 * 5. saveAsTable create a hive table and save the files in HDFS using format parquet and snappy compression
 */
