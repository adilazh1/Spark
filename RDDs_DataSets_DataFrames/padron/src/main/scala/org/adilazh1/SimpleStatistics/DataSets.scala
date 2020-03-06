package org.adilazh1.SimpleStatistics

import java.io.PrintWriter
import org.apache.spark.sql._

import scala.util.Try

/**
 * The objective here is compute the same statistics than RDDs's class, but using DataSets structure,
 * to compare performance.
 */
object DataSets {

  case class Table(COD_DISTRITO:Int, DESC_DISTRITO:String, COD_DIST_BARRIO:Int,DESC_BARRIO:String,
                   COD_BARRIO:Int, COD_DIST_SECCION:Int, COD_SECCION:Int, COD_EDAD_INT:Int, EspanolesHombres:Int,
                   EspanolesMujeres:Int,ExtranjerosHombres:Int,ExtranjerosMujeres:Int)
  def simpleStatistics(spark:SparkSession)={

    val t0 = System.currentTimeMillis()
    import spark.implicits._
    val file  = spark.sparkContext.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/padron/padron.csv")
      .filter(!_.contains("COD")).map(fun).toDS().cache();

    /*
    Total Foreigners
    Total Spanish
    Total Population
    Percent foreigners
    Max Age
    Average Age
    Top 10 districts with more foreigners
    top 10 districts with more foreigners percentage
 */

    val totForeigners = file.select(file("ExtranjerosHombres")+file("ExtranjerosMujeres")).alias("Foreigners")
      .groupBy().sum().first().getLong(0).toInt
    val totSpanish = file.select(file("EspanolesHombres")+file("EspanolesMujeres")).alias("Spanish")
      .groupBy().sum().first().getLong(0).toInt
    val totPopulation = totForeigners + totSpanish
    val percent = 100.0*totForeigners/totPopulation
    val maxAge = file.agg(functions.max("COD_EDAD_INT")).first().getInt(0)
    val avgAge = file.agg(functions.mean("COD_EDAD_INT")).first().getDouble(0)
    val topForeigners = file.select(file("DESC_DISTRITO"),(file("ExtranjerosHombres")+file("ExtranjerosMujeres"))
      .alias("totForeigners")).groupBy("DESC_DISTRITO")
      .sum("totForeigners").sort(functions.desc("sum(totForeigners)"))
    val topSpanish = file.select(file("DESC_DISTRITO"),(file("EspanolesHombres")+file("EspanolesMujeres"))
      .alias("totSpanish")).groupBy("DESC_DISTRITO")
      .sum("totSpanish")

    val join = topForeigners.join(topSpanish,"DESC_DISTRITO").cache()
    val percentages = join.select(join("DESC_DISTRITO"),
      (join("sum(totForeigners)")/(join("sum(totForeigners)")+join("sum(totSpanish)"))).alias("percentage"))
      .sort(functions.desc("percentage"))

    //out
    var out = "totforeigners = " +totForeigners + "\ntotSpanish = " +totSpanish +"\ntotPopulation = " + totPopulation+
      "\nPercent = " + percent +"\nMax(Age) = " + maxAge + "\nAvg(Age) = " + avgAge + "\nTop 10 districts with more foreigners\n"

    val pw = new PrintWriter("/home/cloudera/Documents/simpeStatestics2.txt")

    val iter1 = topForeigners.take(10).toIterator
    val iter2 = percentages.take(10).toIterator
    while(iter1.hasNext){
      out += iter1.next() +"\n"
    }
    out+="Top 10 districts with more foreigners in percentage \n"
    while(iter2.hasNext){
      out += iter2.next() +"\n"
    }
    val t1 = System.currentTimeMillis()
    out += "\n total time = " +(t1-t0)
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

    val table:Table = Table(line(0).toInt,line(1),line(2).toInt,line(3),line(4).toInt,line(5).toInt,line(6).toInt,
      line(7).toInt,EspanolesHombres,EspanolesMujeres,ExtranjerosHombres,ExtranjerosMujeres)
    return table
  }

}
