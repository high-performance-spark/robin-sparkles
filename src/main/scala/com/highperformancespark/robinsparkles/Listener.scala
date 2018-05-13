package com.highperformancespark.robinsparkles

import java.io.FileInputStream
import java.nio.file.Paths

import better.files._
import better.files.File._
import ch.cern.sparkmeasure.StageVals
import ch.cern.sparkmeasure.Utils.ObjectInputStreamWithCustomClassLoader
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class RobinStageListener(sc: SparkContext, override val metricsFileName: String)
    extends ch.cern.sparkmeasure.FlightRecorderStageMetrics(sc.getConf) {
}

class RobinTaskListener(sc: SparkContext, override val metricsFileName: String)
    extends ch.cern.sparkmeasure.FlightRecorderTaskMetrics(sc.getConf) {
  // Hack: we should use HDFS in the future, but requires an SC
}

class MetricsReader(conf: SparkConf, metricsRootDir: String) {
  val appName = conf.getOption("spark.app.name").getOrElse("please_set_app_name")
  //TODO: Should we just store with application ID
  val STAGE_METRICS_SUBDIR = "stage_metrics"
  val TASK_METRICS_SUBDIR = "task_metrics"

  val metricsDir = s"$metricsRootDir/${appName}"

  val stageMetricsDir = s"$metricsDir/$STAGE_METRICS_SUBDIR"
  val taskMetricsDir = s"$metricsDir/$TASK_METRICS_SUBDIR"

  def stageMetricsPath(n: Int): String = {
    s"$metricsDir/run=$n"
  }

  def taskMetricsPath(n: Int): String = {
    s"$taskMetricsDir/run=$n"
  }

//  def numPreviousRuns = {
//    val fullPath = Paths.get(s"$metricsDir/$STAGE_METRICS_DIR")
//    val files = java
//    val ois = new ObjectInputStreamWithCustomClassLoader(new FileInputStream(fullPath))
//    val result = ois.readObject().asInstanceOf[ListBuffer[T]]
//    result
//  }
  def readStageInfo(n : Int) = {
    ch.cern.sparkmeasure.Utils.readSerializedStageMetrics(stageMetricsPath(n))
  }

  def readTaskInfo(n: Int) = {
    ch.cern.sparkmeasure.Utils.readSerializedTaskMetrics(taskMetricsPath(n))
  }

  def getRunInfo(n: Int): Option[List[StageInfo]] = {
    try {
      val stageInfo = readStageInfo(n)
      val taskInfo = readTaskInfo(n)
      val stageMap = stageInfo.map(s => (s.stageId, s)).toMap
      val taskMap = taskInfo.map(s => (s.stageId, s)).groupBy(_._1)
      Some(stageMap.map { case (k, v) =>
        StageInfo(v, taskMap(k).unzip._2)
      }.toList)
    } catch {
      // Hack
      case _ => None
    }
  }
}

class MetricsCollector(sc: SparkContext, metricsRootDir: String)
    extends MetricsReader(sc.getConf, metricsRootDir) {

  // Hack: we should use HDFS in the future, but requires an SC
  stageMetricsDir.toFile.createIfNotExists(true, true)
  taskMetricsDir.toFile.createIfNotExists(true, true)

  def startSparkJobWithRecording(runNumber: Int) = {
    val myTaskListener = new RobinTaskListener(sc,
      taskMetricsPath(runNumber))
    val myStageListener = new RobinStageListener(sc,
      stageMetricsPath(runNumber))
    sc.addSparkListener(myTaskListener)
    sc.addSparkListener(myStageListener)
  }
}

