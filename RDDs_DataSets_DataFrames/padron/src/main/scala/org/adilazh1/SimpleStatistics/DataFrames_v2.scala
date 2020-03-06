package org.adilazh1.SimpleStatistics

import java.io.PrintWriter

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.storage.StorageLevel

object DataFrames_v2 {

  def simpleStatistics(spark:SparkSession)={

    val t0 = System.currentTimeMillis()

    val file= spark.read.options(Map("inferSchema"->"true","delimiter"->";","header"->"true")).
      csv("hdfs://quickstart.cloudera:8020/user/cloudera/padron/padron.csv")
      .na.fill(0,Seq("EspanolesHombres","EspanolesMujeres","ExtranjerosHombres","ExtranjerosMujeres")).cache();

    val population = file.select(file("DESC_DISTRITO"),(file("EspanolesHombres")+file("EspanolesMujeres")).as("spanish"),
        (file("ExtranjerosHombres")+file("ExtranjerosMujeres")).as("foreigners")).groupBy("DESC_DISTRITO").sum()
      .sort(functions.desc("sum(foreigners)")).toDF()

    val statestics = file.agg((functions.sum("EspanolesHombres")+functions.sum("EspanolesMujeres")).as("totSpanish"),
      (functions.sum("ExtranjerosHombres")+functions.sum("ExtranjerosMujeres")).as("totForeigners"),
      functions.max("COD_EDAD_INT").as("maxAge"),functions.mean("COD_EDAD_INT").as("avgAge")).cache()

    val percentageByDistrict = population.select(population("DESC_DISTRITO"),
      (population("sum(foreigners)")/(population("sum(foreigners)") + population("sum(spanish)"))).as("percentage"))
      .sort(functions.desc("percentage")).toDF()

    val totSpanish = statestics.first().getLong(0).toInt
    val totForeigners = statestics.first().getLong(1).toInt
    val percentage = 100.0*totForeigners/(totForeigners+totSpanish)
    val maxAge=  statestics.first().getInt(2)
    val avgAge = statestics.first().getDouble(3)

    //out
    var out = s"totforeigners = ${totForeigners} \ntotSpanish = ${totSpanish} " +
      s"\ntotPopulation = ${totForeigners + totSpanish} \nPercent = ${percentage} \nmaxAge = ${maxAge}" +
      s"\navgAge = ${avgAge} \ntop 10 districts with more foreigners \n"

    val iter1 = population.take(10).toIterator
    val iter2 = percentageByDistrict.take(10).toIterator
    while (iter1.hasNext) {
      out += s"${iter1.next()} \n"
    }
    out += "Top 10 districts with more foreigners in percentage \n"
    while (iter2.hasNext) {
      out += s"${iter2.next()} \n"
    }
    val pw = new PrintWriter("/home/cloudera/Documents/simpeStatestics3_v2.txt")
    val t1 = System.currentTimeMillis()
    out += "\n total time = " + (t1 .-(t0))
    pw.write(out)
    pw.close()

  }
}
