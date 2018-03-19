package com.highperformancespark.robinsparkles

import java.nio.file.FileSystem

import com.highperformancespark.robinsparkles.CountingLocalApp.conf
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
    .setAppName("my_awesome_app").set("spark.num.executors", "1")

  val (newConf: SparkConf, id: Int) = Runner.getOptimizedConf(metricsDir, conf)
  val sc = SparkContext.getOrCreate(newConf)

  Runner.run(sc, id, metricsDir, inputFile, outputFile)
}

/**
  * Use this when submitting the app to a cluster with spark-submit
  * */
object CountingApp extends App{
  val (inputFile, outputFile) = (args(0), args(1))

  // spark-submit command should supply all necessary config elements
  Runner.run(SparkContext.getOrCreate(), 0, "/tmp", inputFile, outputFile)
}

object Runner {

  def getOptimizedConf(metricsDir: String, conf: SparkConf):
      (SparkConf, Int) = {
    val metricsReader = new MetricsReader(conf, metricsDir)
    // Load all of the previous runs until one isn't found
    val prevRuns = Stream.from(0)
      .map(id => metricsReader.getRunInfo(id))
      .takeWhile(_.isDefined)
      .map(_.get)
    // Hack right now we only look at the previous run
    val prevRunOption = prevRuns.lastOption
    // Only compute the partitions if there is historical data
    val pOption  = prevRunOption.map(prevRun => ComputePartitions.apply(conf).fromStageMetric(prevRun))

    pOption.foreach(p => (conf.set("spark.default.parallelism", p.toString)))
    (conf, prevRuns.length)
  }
  def run(sc: SparkContext, id: Int, metricsDir: String, inputFile: String, outputFile: String): Unit = {
    val metricsCollector = new MetricsCollector(sc, metricsDir)
    metricsCollector.startSparkJobWithRecording(id)


    val rdd = sc.textFile(inputFile)
    val counts = WordCount.withStopWordsFiltered(rdd)

    counts.saveAsTextFile(outputFile)
  }
}
