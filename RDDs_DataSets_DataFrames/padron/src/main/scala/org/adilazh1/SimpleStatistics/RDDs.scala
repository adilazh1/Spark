package org.adilazh1.SimpleStatistics

import java.io.PrintWriter

import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.util.Try

object RDDs {

  def simpleStatistics(spark:SparkSession)={

    //read the file from HDFS

    val t0 = System.currentTimeMillis()
    val file  = spark.sparkContext.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/padron/padron.csv");
    file.persist(StorageLevel.MEMORY_ONLY)
    val file1 = file.filter(f => !f.contains("COD"))

    //compute max and average Age
    val colAge = file1.map(f => f.split(";")(7).replaceAll("\"","").toInt)
    .persist(StorageLevel.MEMORY_ONLY)
    val maxAge = colAge.max()
    val avgAge = colAge.mean()

    //district with more foreigners
    val foreignByDistrict = file1.map(f=> (f.split(";")(1),
      f.split(";")(10).replace("\"","") + ";"+
      f.split(";")(11).replace("\"","")))
      .map(f => (f._1 , fun(f._2))).reduceByKey(_+_).sortBy(_._2,false);

    //Compute total of foreigners
    val totForeign = foreignByDistrict.map(_._2).sum()

    //percentage of foreigners
    //we need to compute spanish total
    val spanishByDistrict = file1.map(f=> (f.split(";")(1),
      f.split(";")(8).replace("\"","") + ";"+
        f.split(";")(9).replace("\"",""))).
      map(f => (f._1 , fun(f._2))).reduceByKey(_+_).sortBy(_._2,false);

    val totSpanish = spanishByDistrict.map(_._2).sum()
    //percentage
    val percent = 100.0*totForeign/(totForeign+totSpanish)

    //percentage by District
    //we need to compute a join using the key value
    val totalP = foreignByDistrict.join(spanishByDistrict)
    val totPercentage = totalP.map(f=> (f._1,100.0*f._2._1/(f._2._1+f._2._2) )).sortBy(_._2,false)

    //Save output into a file
    val iter1 = foreignByDistrict.take(10).toIterator
    val iter2 = totPercentage.take(10).toIterator
    var out ="totforeigners = "+ totForeign + "\ntotSpanish = " + totSpanish + "\ntotPopulation = "+(totForeign+totSpanish)+
      "\nPercent = " +percent + "\nMax(Age) = "+maxAge +"\nAvg(Age) = "+avgAge + "\nTop 10 districts with more foreigners" + "\n";
    while(iter1.hasNext){
      out += iter1.next() +"\n"
    }
    out+="Top 10 districts with more foreigners in percentage \n"
    while(iter2.hasNext){
      out += iter2.next() +"\n"
    }
    val pw = new PrintWriter("/home/cloudera/Documents/simpeStatestics.txt")
    val t1 = System.currentTimeMillis()
    out += "\n total time = " + (t1 .-(t0))
    pw.write(out)
    pw.close()

    //Next activities: combine this statistics with others like commerce in Madrid etc.
    //look for correlations
  }
//
  def fun(a:String):Int={
    var a1 = 0;
    var b1 = 0;
    val value = a.trim.split(";")
    if(Try(value(0).toInt).isSuccess) a1 = value(0).toInt
    if(Try(value(1).toInt).isSuccess) b1 = value(1).toInt
    return a1+b1;
  }

}
