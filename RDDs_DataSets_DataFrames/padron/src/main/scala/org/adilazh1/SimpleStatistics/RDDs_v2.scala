package org.adilazh1.SimpleStatistics

import java.io.PrintWriter

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.util.Try

object RDDs_v2 {

  def simpleStatistics(spark:SparkSession)= {

    val t0 = System.currentTimeMillis()
    //read csv from hdfs like textfile
    val file = spark.sparkContext.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/padron/padron.csv")
      .filter(f => !f.contains("COD"));
    file.persist(StorageLevel.MEMORY_ONLY)

    //compute max and average Age
    val colAge = file.map(f => f.split(";")(7).replaceAll("\"", "").toInt)
      .persist(StorageLevel.MEMORY_ONLY)
    val maxAge = colAge.max()
    val avgAge = colAge.mean()

    //district with more foreigners, total people by district.
    //Try to do only one reduce and not twice like the last example
    val people = file.map(f => (f.split(";")(1).replace("\"", ""), (
      f.split(";")(8).replace("\"", "") + ";" +
        f.split(";")(9).replace("\"", ""),
      f.split(";")(10).replace("\"", "") + ";" +
        f.split(";")(11).replace("\"", ""))))
      .map(f => (f._1.trim, (sum(f._2._1), sum(f._2._2))))
      .reduceByKey {
        case ((a1, a2), (b1, b2)) => (a1 + b1, a2 + b2)
      }.sortBy(_._2._2, false).persist(StorageLevel.MEMORY_ONLY);

    //compute sum and percentage
    val totSpanish = people.map(_._2._1).sum()
    val totForeigners = people.map(_._2._2).sum()
    val percentage = 100.0 * totForeigners / (totForeigners + totSpanish)

    //Percentage by district
    val foreignersByDistrict = people.mapValues(f => 100.0*f._2 / (f._2 + f._1)).sortBy(_._2, false)

    //out
    var out = s"totforeigners = ${totForeigners} \ntotSpanish = ${totSpanish} " +
      s"\ntotPopulation = ${totForeigners + totSpanish} \nPercent = ${percentage} \nmaxAge = ${maxAge}" +
      s"\navgAge = ${avgAge} \ntop 10 districts with more foreigners \n"

    val iter1 = people.take(10).toIterator
    val iter2 = foreignersByDistrict.take(10).toIterator
    while (iter1.hasNext) {
      out += s"${iter1.next()} \n"
    }
    out += "Top 10 districts with more foreigners in percentage \n"
    while (iter2.hasNext) {
      out += s"${iter2.next()} \n"
    }
    val pw = new PrintWriter("/home/cloudera/Documents/simpeStatestics_v2.txt")
    val t1 = System.currentTimeMillis()
    out += "\n total time = " + (t1 .-(t0))
    pw.write(out)
    pw.close()
  }

  def sum(a:String):Int={
    var a1 = 0
    var b1 = 0
    val value = a.trim.split(";")
    if(Try(value(0).toInt).isSuccess) a1 = value(0).toInt
    if(Try(value(1).toInt).isSuccess) b1 = value(1).toInt
    return a1+b1;
  }
}
