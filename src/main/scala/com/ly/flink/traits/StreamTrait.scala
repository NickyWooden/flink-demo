package com.ly.flink.traits

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

trait StreamTrait {
  val see: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  def init() = {

  }
  def run()

  def stop() = {

  }

}
