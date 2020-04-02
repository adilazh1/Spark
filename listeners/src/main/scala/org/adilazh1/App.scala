package org.adilazh1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
/**
 * @author ${user.name}
 */
object App {

  
  def main(args : Array[String]) {

    val spark = SparkSession.builder.appName("Listeners").master("local[*]")
      .config("spark.sql.warehouse-dir","/user/hive/warehouse")
      .config("hive.exec.scratchdir","/tmp/tmpHive")
      .config("hive.metastore.uris","thrift://localhost:9083")
      .enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.ERROR)

    val test = TaskMetrics(spark)


    val df = spark.read.parquet("hdfs://quickstart.cloudera:8020/user/hive/warehouse/bicing.db/bicing_1")//transformation

    test.runAndMeasure("Read",df.show())


    df.createOrReplaceTempView("table")
    df.cache()
    val sql = spark.sqlContext.sql("SELECT * FROM table WHERE station_id = 1")

    test.runAndMeasure("Read",sql) //lee toda la tabla para seleccionar un registro. Secuential acces

    test.runAndMeasure("Write",sql.write.mode("overwrite").saveAsTable("bicing.bicing_5"))

    spark.close()
  }

}
/**
 * Conclusiones:
 * 1. SparkListener aporta muchas clases, salida de diferentes eventos.https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.scheduler.SparkListener
 * 2. Crear una clase que extiende de SparkListener nos permite sobre-escribir métodos que controlan eventos, como onTaskEnd, que nos permite acceder a métricas de una tarea
 *    una vez finalizada.
 * 3. com.databriks aporta dos clases que generan una dataframe de métricas
 *    Para usarla:
 *    val t = new TaskMetricsExplorer(spark)
 *    sparkSession.sparkContext.addSparkListener(t)
 *
 *    t.runAndMeasure(accion), ejecuta la acción y devuelve DataFrame con métricas
 * Nota: Este ejemplo ha sido adaptado del proyecto  https://github.com/LucaCanali/sparkMeasure
 */