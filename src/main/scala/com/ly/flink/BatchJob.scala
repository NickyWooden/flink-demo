package com.ly.flink

import org.apache.flink.api.scala._


object BatchJob {

  def main(args: Array[String]) {
    // set up the batch execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment


    // execute program
    env.execute("Flink Batch Scala API Skeleton")
  }
}
