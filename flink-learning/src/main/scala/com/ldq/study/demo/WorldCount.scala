package com.ldq.study.demo

import org.apache.flink.api.scala._


object WorldCount {

  def main(args: Array[String]){
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.fromElements("hello java hello,flink","jack queen")

    val value = text.flatMap(x => x.split(","))
      .map((_, 1))
    value.print

    val counts = text.flatMap{ _.toLowerCase.split("\\W+") }
      .map{(_, 1)}
      .groupBy(0)
      .sum(1)

    counts.print
  }
}
