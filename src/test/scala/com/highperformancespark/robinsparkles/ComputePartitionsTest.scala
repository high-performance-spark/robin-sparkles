package com.highperformancespark.robinsparkles


import org.apache.spark.SparkConf
import org.scalatest.FunSuite


class ComputePartitionsTest extends FunSuite {

  test("If partitions increased and stage  time decrease, then we should increase partitions "){

    val first = StageInfo(
      executorCPUTime = 105,
      stageTime = 105,
      totalInputSize = 1024 * 1024 * 1024 * 2 * 3,
      numExecutors = 3,
      shuffleSize = ShuffleSize(20, 30),
      taskMetrics = List.fill(19)(Task(11)))

    val second = StageInfo(
      executorCPUTime = 90,
      stageTime = 95,
      totalInputSize = 1024 * 1024 * 1024 * 3,
      numExecutors = 3,
      shuffleSize = ShuffleSize(20, 30),
      taskMetrics = List.fill(20)(Task(10)))

    implicit val sparkConf =  new SparkConf().setAll(Map(
      "spark.executor.instances" -> "4",
      "spark.executor.cores" -> "2",
      "spark.executor.memory" -> s"${Math.round(1024*2/(0.6*0.5))}m"
    ))

    val partitions = ComputePartitions(sparkConf).fromStageMetricSharedCluster(List(first, second))
    assert(partitions > second.numPartitionsUsed)
  }

}
