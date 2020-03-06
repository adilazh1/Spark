package org.adilazh1.SimpleStatistics

import java.io.PrintWriter

import org.apache.spark.sql.{SparkSession, functions}

import scala.util.Try

object DataSets_v2 {

  case class Table(COD_DISTRITO:Int, DESC_DISTRITO:String, COD_DIST_BARRIO:Int,DESC_BARRIO:String,
                   COD_BARRIO:Int, COD_DIST_SECCION:Int, COD_SECCION:Int, COD_EDAD_INT:Int, EspanolesHombres:Int,
                   EspanolesMujeres:Int,ExtranjerosHombres:Int,ExtranjerosMujeres:Int)

  def simpleStatistics(spark:SparkSession)={

    val t0 = System.currentTimeMillis()
    import spark.implicits._

    val file  = spark.sparkContext.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/padron/padron.csv")
      .filter(!_.contains("COD")).map(fun).toDS().cache();

    val population = file.select(file("DESC_DISTRITO"),(file("EspanolesHombres")+file("EspanolesMujeres")).as("spanish"),
                                 (file("ExtranjerosHombres")+file("ExtranjerosMujeres")).as("foreigners"))
      .groupBy("DESC_DISTRITO").sum().sort(functions.desc("sum(foreigners)"))

    val statestics = file.agg((functions.sum("EspanolesHombres")+functions.sum("EspanolesMujeres")).as("totSpanish"),
      (functions.sum("ExtranjerosHombres")+functions.sum("ExtranjerosMujeres")).as("totForeigners"),
      functions.max("COD_EDAD_INT").as("maxAge"),functions.mean("COD_EDAD_INT").as("avgAge")).cache()

    val percentageByDistrict = population.select(population("DESC_DISTRITO"),
      (population("sum(foreigners)")/(population("sum(foreigners)") + population("sum(spanish)"))).as("percentage"))
      .sort(functions.desc("percentage"))

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
    val pw = new PrintWriter("/home/cloudera/Documents/simpeStatestics2_v2.txt")
    val t1 = System.currentTimeMillis()
    out += "\n total time = " + (t1 .-(t0))
    pw.write(out)
    pw.close()

  }

  def fun(inputRow:String):Table = {
    val line = inputRow.replaceAll("\"","").split(";")
    var EspanolesHombres = 0
    var EspanolesMujeres = 0
    var ExtranjerosHombres = 0
    var ExtranjerosMujeres = 0
    if(Try(line(8).toInt).isSuccess) EspanolesHombres = line(8).toInt
    if(Try(line(9).toInt).isSuccess) EspanolesMujeres = line(9).toInt
    if(Try(line(10).toInt).isSuccess) ExtranjerosHombres = line(10).toInt
    if(Try(line(11).toInt).isSuccess) ExtranjerosMujeres = line(11).toInt

    val table:Table = Table(line(0).toInt,line(1).trim,line(2).toInt,line(3).trim,line(4).toInt,line(5).toInt,line(6).toInt,
      line(7).toInt,EspanolesHombres,EspanolesMujeres,ExtranjerosHombres,ExtranjerosMujeres)
    return table
  }
}
