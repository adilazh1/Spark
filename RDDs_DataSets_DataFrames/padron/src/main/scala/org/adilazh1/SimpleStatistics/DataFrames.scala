package org.adilazh1.SimpleStatistics

import java.io.PrintWriter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object DataFrames {
  def simpleStatistics(spark:SparkSession)={

    val t0 = System.currentTimeMillis()

    //We lets Spark infer schema, but for more control about dataType, is better define explicitly the Schema.
    //However, with printSchem function we can check the schema.
    val file= spark.read.options(Map("inferSchema"->"true","delimiter"->";","header"->"true")).
      csv("hdfs://quickstart.cloudera:8020/user/cloudera/padron/padron.csv").na.fill(0);

    file.persist(StorageLevel.MEMORY_ONLY)
    file.createOrReplaceTempView("padron")


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

    spark.sql("SELECT DESC_DISTRITO, SUM(ExtranjerosHombres + ExtranjerosMujeres) as foreigners from padron group by DESC_DISTRITO order by foreigners desc ").createOrReplaceTempView("Foreigners")
    spark.sql("SELECT DESC_DISTRITO, SUM(EspanolesHombres + EspanolesMujeres) as spanish from padron group by DESC_DISTRITO ").createOrReplaceTempView("Spanish")

    val totForeigners = spark.sql("SELECT SUM(foreigners) as foreigners from Foreigners").cache()
      .first().getLong(0).toInt
    val totSpanish = spark.sql("SELECT SUM(spanish) as spanish from Spanish").cache()
      .first().getLong(0).toInt
    val totPopulation = totForeigners+totSpanish
    val percent = 100.0*totForeigners/(totPopulation)

    val maxAge = spark.sql("select max(COD_EDAD_INT) as maxAge from padron").cache().first().getInt(0)
    val avgAge = spark.sql("select avg(COD_EDAD_INT) as maxAge from padron").first().getDouble(0)

    val join = spark.sql("SELECT A.*,B.spanish,100.0*(A.foreigners/(A.foreigners+B.spanish)) as percentage from Foreigners A, Spanish B  where A.DESC_DISTRITO = B.DESC_DISTRITO order by percentage desc  ")
    val top10Foreigners = spark.sql("SELECT * FROM Foreigners LIMIT 10")

    //save the results
    var out = "totforeigners = " +totForeigners + "\ntotSpanish = " +totSpanish +"\ntotPopulation = " + totPopulation+
      "\nPercent = " + percent +"\nMax(Age) = " + maxAge + "\nAvg(Age) = " + avgAge + "\nTop 10 districts with more foreigners\n"

    val pw = new PrintWriter("/home/cloudera/Documents/simpeStatestics3.txt")

    val iter1 = top10Foreigners.take(10).toIterator
    val iter2 = join.take(10).toIterator
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
}
