package com.highperformancespark.robinsparkles


import org.apache.spark.SparkConf
import org.scalatest.FunSuite


class ComputePartitionsTest extends FunSuite {

  test("If partitions increased and stage  time decrease, then we should increase partitions "){

    val first = WebUIInput(
      stageTime = 105,
      taskMetrics = List.fill(19)(Task(11)),
      executorSummaries = List.fill(3)(ExecutorSummary(1024 * 1024 * 1024 * 2)))

    val second = WebUIInput(
      stageTime = 90,
      taskMetrics = List.fill(20)(Task(10)),
      executorSummaries = List.fill(3)(ExecutorSummary(1024 * 1024 * 1024))
    )

    implicit val sparkConf =  new SparkConf().setAll(Map(
      "spark.executor.instances" -> "4",
      "spark.executor.cores" -> "2",
      "spark.executor.memory" -> s"${Math.round(1024*2/(0.6*0.5))}m"
    ))

    val partitions = ComputePartitions().fromStageMetric(List(first, second))
    assert(partitions > second.numPartitionsUsed)

  }

}