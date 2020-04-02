package org.adilazh1

import org.apache.spark.scheduler._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

case class TaskValsRead(taskName: String, jobId: Long, stageId: Int,
                    duration: Long, executorId: String, host: String,
                    successful: Boolean, executorCpuTime: Long, jvmGCTime: Long,
                    recordsRead: Long, bytesRead: Long)

case class TaskValsWrite(taskName: String, jobId: Long, stageId: Int,
                        duration: Long, executorId: String, host: String,
                        successful: Boolean, executorCpuTime: Long, jvmGCTime: Long,
                        recordsWritten: Long, bytesWritten: Long)


class OnSparkScheduler(sparkSession: SparkSession) extends SparkListener {

  val taskMetricsDataRead: ListBuffer[TaskValsRead] = ListBuffer.empty[TaskValsRead]
  val taskMetricsDataWrite: ListBuffer[TaskValsWrite] = ListBuffer.empty[TaskValsWrite]


  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {


    val jobId = taskEnd.taskInfo.taskId
    val duration = taskEnd.taskInfo.finishTime - taskEnd.taskInfo.launchTime

    val currentTaskRead = TaskValsRead("Read task", jobId, taskEnd.stageId, duration, taskEnd.taskInfo.executorId, taskEnd.taskInfo.host,
      taskEnd.taskInfo.successful, taskEnd.taskMetrics.executorCpuTime / 1000000, taskEnd.taskMetrics.jvmGCTime,
      taskEnd.taskMetrics.inputMetrics.recordsRead, taskEnd.taskMetrics.inputMetrics.bytesRead)
    taskMetricsDataRead += currentTaskRead


    val currentTaskWrite = TaskValsWrite("Write task", jobId, taskEnd.stageId, duration, taskEnd.taskInfo.executorId, taskEnd.taskInfo.host,
      taskEnd.taskInfo.successful, taskEnd.taskMetrics.executorCpuTime/ 1000000, taskEnd.taskMetrics.jvmGCTime,
      taskEnd.taskMetrics.outputMetrics.recordsWritten, taskEnd.taskMetrics.outputMetrics.bytesWritten)
    taskMetricsDataWrite += currentTaskWrite

  }
}
  case class TaskMetrics(sparkSession: SparkSession,taskType:String = "Read") {

    val onSparkSchedule = new OnSparkScheduler(sparkSession)
    sparkSession.sparkContext.addSparkListener(onSparkSchedule)


    def createTaskMetricsReadDF(nameTempView: String = "PerfTaskMetricsRead"): DataFrame = {
      import sparkSession.implicits._
      val resultDF = onSparkSchedule.taskMetricsDataRead.toDF
      resultDF.createOrReplaceTempView(nameTempView)

      resultDF
    }

    def createTaskMetricsWriteDF(nameTempView: String = "PerfTaskMetricsWrite"): DataFrame = {
      import sparkSession.implicits._
      val resultDF = onSparkSchedule.taskMetricsDataWrite.toDF
      resultDF.createOrReplaceTempView(nameTempView)

      resultDF
    }


def aggregateTaskMetricsRead(nameTempView: String = "PerfTaskMetricsRead"): DataFrame = {
  sparkSession.sql(s"select sum(duration) as durationTot, " +
    s"sum(executorCpuTime) as executroCpuTimeTot, sum(jvmGCTime) as jvmGCTimeTot, " +
    s"sum(recordsRead) as recordsReadTot, sum(bytesRead) as bytesReadTot " +
    s"from $nameTempView ")
}


def aggregateTaskMetricsWrite(nameTempView: String = "PerfTaskMetricsWrite"): DataFrame = {
  sparkSession.sql(s"select sum(duration) as durationTot, " +
    s"sum(executorCpuTime) as executroCpuTimeTot, sum(jvmGCTime) as jvmGCTimeTot, " +
    s"sum(recordsWritten) as recordsWrittenTot, sum(bytesWritten) as bytesWrittenTot " +
    s"from $nameTempView ")
}

    def report(taskType:String = "Read"): String = {
      var result = ListBuffer[String]()

      result = result :+ (s"\n********* Task Type: ${taskType} ***********")
      result = result :+ (s"Scheduling mode = ${sparkSession.sparkContext.getSchedulingMode.toString}")
      result = result :+ (s"Spark Contex default degree of parallelism = ${sparkSession.sparkContext.defaultParallelism}")
      result = result :+ ("Aggregated Spark task metrics:")

      /** Print a summary of the task metrics. */
      val aggregateDF = matchDF(taskType)
      val aggregateValues = aggregateDF.take(1)(0).toSeq

      val cols = aggregateDF.columns
      result = result :+ (cols zip aggregateValues)
        .map {
          case (n: String, v: Long) => Utils.reformat(n,v)
          case (n: String, null) => n + " => null"
          case (_, _) => ""
        }.mkString("\n")
      result.mkString("\n")

    }

    def matchDF(taskType: String= "Read"):DataFrame = taskType match{

      case "Read" => createTaskMetricsReadDF(s"PerfTaskMetrics$taskType")
        val aggregateDF = aggregateTaskMetricsRead(s"PerfTaskMetrics$taskType")
        aggregateDF
      case "Write" =>  createTaskMetricsWriteDF(s"PerfTaskMetrics$taskType")
        val aggregateDF = aggregateTaskMetricsWrite(s"PerfTaskMetrics$taskType")
        aggregateDF

    }
    def printReport(typeTask:String="Read"): Unit = {
      println(report(typeTask))
    }

    def runAndMeasure[T](typeTask:String = "Read",f: => T): T = {
      val startTime = System.nanoTime()
      val ret = f
      val endTime = System.nanoTime()
      println(s"Time taken: ${(endTime - startTime) / 1000000} ms")
      printReport(typeTask)
      ret
    }

}
