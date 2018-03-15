package com.highperformancespark.robinsparkles

import java.nio.file.FileSystem

import com.highperformancespark.robinsparkles.CountingLocalApp.conf
import org.apache.hadoop.metrics2.MetricsCollector
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

/**
  * Use this to test the app locally, from sbt:
  * sbt "run inputFile.txt outputFile.txt"
  *  (+ select CountingLocalApp when prompted)
  */
object CountingLocalApp extends App{
  println("ARGS ARE " + args.mkString(", "))

  val (inputFile, outputFile, metricsDir) = if(args.length > 2)
    (args(0), args(1), args(3))
  else
    ("src/test/resources/Words.txt"
      , "tmp/output", "/tmp/metrics")
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("my awesome app").set("spark.num.executors", "1")

 val newConf = Runner.getOptimizedConf(metricsDir, conf)

  Runner.run(newConf, inputFile, outputFile)
}

/**
  * Use this when submitting the app to a cluster with spark-submit
  * */
object CountingApp extends App{
  val (inputFile, outputFile) = (args(0), args(1))

  // spark-submit command should supply all necessary config elements
  Runner.run(new SparkConf(), inputFile, outputFile)
}

object Runner {

  def getOptimizedConf(metricsDir : String, conf : SparkConf) : SparkConf = {
    val metricsCollector = new MetricsCollector(metricsDir)
    val firstRun = metricsCollector.getRunInfo(0)
    val secondRun = metricsCollector.getRunInfo(1)
    val prevRuns = firstRun ++ secondRun
    val p = ComputePartitions.apply(conf).fromStageMetric(prevRuns)

    metricsCollector.startSparkJobWithRecording(
      conf
        .set("spark.default.parallelism", p.toString)
        .set("spark.local.dir", metricsDir),
       prevRuns.length)
  }
  def run(conf: SparkConf, inputFile: String, outputFile: String): Unit = {

    val sc = new SparkContext(conf)

    println(" local dir is " + sc.getConf.get("spark.local.dir"))

    val rdd = sc.textFile(inputFile)
    val counts = WordCount.withStopWordsFiltered(rdd)

    counts.saveAsTextFile(outputFile)
  }
}
