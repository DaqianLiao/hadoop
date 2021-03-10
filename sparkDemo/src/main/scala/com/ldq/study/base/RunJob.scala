package com.ldq.study.base

import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object RunJob {
  def main(args: Array[String]): Unit = {


    val sparkConf: SparkConf = new SparkConf().setAppName("Master").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val rdd = sc
      .textFile("D:\\demo.txt")
      .map(line => {
        val arr: Array[String] = line.split(",")
        (arr(0), arr(1))
      })

    rdd.count()
    val func = (iter: Iterator[(String, String)]) => {
      var count = 0
      iter.foreach(_ => {
        count += 1
      })
      (TaskContext.getPartitionId(), count)
    }

    val tuples: Array[(Int, Int)] = sc.runJob(rdd, func)
    tuples.foreach(println)
    sc.stop()
  }

}
