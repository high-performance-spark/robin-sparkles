package com.highperformancespark.robinsparkles

import org.apache.spark.SparkConf
import org.slf4j.{Logger, LoggerFactory}

//These are placeholders for some kind of executor information or tasks class since those sparks objects are private
case class Task(totalTime: Int) //TODO Replace this with the actual stuff or spark object
case class ExecutorSummary(inputBytes : Long)

/**
 * I have tried to put the logic for computing things from the UI metrics directly in this case clss so that we can be
 * flexible about which of those metrics we collect
 */
case class WebUIInput(
  stageTime: Int,
  private val taskMetrics: List[Task],
  private val executorSummaries: List[ExecutorSummary]){

  val numPartitionsUsed: Int = taskMetrics.size //TODO: Subtract retries
  val numExectutors: Int = executorSummaries.size
  /**
   * Sum of the input bytes read on each executor
   */
  val totalInputSize : Double =
    executorSummaries.foldRight(0.0)((e1, acc)=> (e1.inputBytes/(1024.0*1024.0))+ acc)

  val totalTaskTime: Int = taskMetrics.foldRight(0)((t, b) => b + t.totalTime)

  val totalTasksRun: Int = taskMetrics.length

}

case class ComputePartitions()(implicit val sparkConf: SparkConf) {
  implicit val logger: Logger = LoggerFactory.getLogger(classOf[ComputePartitions])
  final val TASK_OVERHEAD_MILLI = 10

  def fromStageMetric(previousRuns: List[WebUIInput]): Int = {
    val concurrentTasks = possibleConcurrentTasks()
    previousRuns match {
      case Nil => concurrentTasks
      case first :: second :: tail =>
        val inputTaskSize = second.totalInputSize
        val taskMemoryMb = availableTaskMemoryMB()
        val floor: Int = Math.max(Math.round(inputTaskSize/taskMemoryMb).toInt, concurrentTasks)
        val execTime = executorIdleTime(second)
        if(execTime > 0){
          if(morePartitionsIsBetter(first,second)){
            Seq(floor, first.numPartitionsUsed, second.numPartitionsUsed).max + second.numExectutors
          }else{
            second.numPartitionsUsed
          }
        }else{
          //Wow that is incredible!
          logger.info("Wow!Your tasks are distributed amongst the executors with maximum efficiency. Don't change a thing")
          Math.max(floor, second.totalTasksRun)
        }
    }
  }

  //TODO: What is the best way to calculate this if using dynamic allocation
  def possibleConcurrentTasks(): Int = {
    sparkConf.getInt("spark.executor.cores", 1) * sparkConf.getInt("spark.num.executors", 1)
  }



  /**
   * The compute space on one executor devided by the number of cores
   * spark.executor.memory * spark.memory.fraction * (1-spark.storageFraction)/spark.executor.cores
   * @return
   */
  def availableTaskMemoryMB(): Double = {
    val execMem = sparkConf.getSizeAsMb("spark.executor.memory")
    val memFraction = sparkConf.getDouble("spark.memory.fraction", 0.6)
    val storageFraction = sparkConf.getDouble("spark.memory.storageFraction", 0.5)
    val nonStorage = 1-storageFraction
    val cores = sparkConf.getInt("spark.executor.cores", 1) //We should probably fail here?
    Math.ceil(execMem * memFraction * nonStorage / cores)
  }


  def executorIdleTime(webUIInput: WebUIInput): Int = {
    val executorStageTime = webUIInput.stageTime * webUIInput.numExectutors
    executorStageTime - webUIInput.totalTaskTime
  }

  /**
   * Compare to stages, return true if the one which uses more partitions had a shorter stage time
   */
  def morePartitionsIsBetter(first: WebUIInput, second : WebUIInput) : Boolean = {
    val morePartitions :: lessPartitions :: _ = List(first,second).sortBy(_.numPartitionsUsed)
    morePartitions.stageTime > lessPartitions.stageTime
  }

}

