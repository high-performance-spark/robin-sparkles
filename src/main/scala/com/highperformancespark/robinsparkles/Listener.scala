package com.highperformancespark.robinsparkles

import java.io.FileInputStream
import java.nio.file.Paths

import ch.cern.sparkmeasure.StageVals
import ch.cern.sparkmeasure.Utils.ObjectInputStreamWithCustomClassLoader
import com.sun.javafx.font.Metrics
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class MetricsCollector(metricsDir: String) {
  //TODO: Should we just store with application ID
  val STAGE_METRICS_DIR = "stage_metrics"
  val TASK_METRICS_DIR = "task_metrics"
  def stageMetricsPath(n : Int) : String = {
    s"$metricsDir/$STAGE_METRICS_DIR/app_$n"
  }
  def taskMetricsPath(n : Int): String = {
    s"$metricsDir/$TASK_METRICS_DIR/app_$n"
  }

//  def numPreviousRuns = {
//    val fullPath = Paths.get(s"$metricsDir/$STAGE_METRICS_DIR")
//    val files = java
//    val ois = new ObjectInputStreamWithCustomClassLoader(new FileInputStream(fullPath))
//    val result = ois.readObject().asInstanceOf[ListBuffer[T]]
//    result
//  }

  def startSparkJobWithRecording(sparkConf : SparkConf, runNumber : Int): SparkConf = {
    sparkConf.set(
      "spark.extraListeners", "ch.cern.sparkmeasure.FlightRecorderStageMetrics")
    sparkConf.set("spark.extraListeners",
      "ch.cern.sparkmeasure.FlightRecorderStageMetrics")

    sparkConf.set("spark.executorEnv.stageMetricsFileName" ,
      metricsDir)
    sparkConf
  }

  def readStageInfo(n : Int) = {
    ch.cern.sparkmeasure.Utils.readSerializedStageMetrics(stageMetricsPath(n))
  }

  def readTaskInfo(n: Int) = {
    ch.cern.sparkmeasure.Utils.readSerializedTaskMetrics(taskMetricsPath(n))
  }

  def getRunInfo(n : Int) : List[WebUIInput] = {
    try{
      val stageInfo = readStageInfo(n)
      val taskInfo = readTaskInfo(n)
      val stageMap = stageInfo.map(s => (s.stageId, s)).toMap
      val taskMap = taskInfo.map(s => (s.stageId, s)).groupBy(_._1)
      stageMap.map { case (k, v) =>
        WebUIInput(v, taskMap(k).unzip._2)
      }.toList

    }catch {
      case t : Throwable => println(s"Failed to get metrics ${t.getMessage}")
        List[WebUIInput]()
    }
  }
}

