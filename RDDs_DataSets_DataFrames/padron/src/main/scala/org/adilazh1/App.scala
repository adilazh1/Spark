package org.adilazh1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 * The objective of this exercise is compare performance between RDDs and DataFrames&DataSets
 * and also using Hive querying.
 * To do that we will develop a App to compute simple statistics over a file downloaded from
 * https://datos.madrid.es/portal/site/egob/
 * Search "padron Monicipal"
 */
object App {

  def main(args : Array[String]) ={

    //create a Spark Session
    val spark = SparkSession.builder().appName("padron")
      .master("local[*]")
      //.master("spark://quickstart.cloudera:7077") its will not work because, we are not use a true cluster, Spark will connect to localhost and so fails
      .getOrCreate();

    Logger.getRootLogger.setLevel(Level.ERROR)
    // Simple Statestics using RDDs

    if(args(0).equals("RDDs")) SimpleStatistics.RDDs.simpleStatistics(spark)
    else if (args(0).equals("dataSets")) SimpleStatistics.DataSets.simpleStatistics(spark)
    else if (args(0).equals("dataFrames")) SimpleStatistics.DataFrames.simpleStatistics(spark);

    /*
    The reason of this second version is to improve  a little the code & performance.
     */
    if(args(0).equals("RDDs_v2")) SimpleStatistics.RDDs_v2.simpleStatistics(spark)
    else if (args(0).equals("dataSets_v2")) SimpleStatistics.DataSets_v2.simpleStatistics(spark)
    else if (args(0).equals("dataFrames_v2")) SimpleStatistics.DataFrames_v2.simpleStatistics(spark);



    // close the Spark session
    spark.close();

  }

}
