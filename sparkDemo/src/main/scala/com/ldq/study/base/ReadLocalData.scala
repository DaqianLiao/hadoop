package com.ldq.study.base

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession;


object ReadLocalData {
  def sparkContext() = {
    val conf = new SparkConf().setAppName("ReadLocalData")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val path = "data/mllib/gmm_data.txt"

    val read = sc.textFile(path)
    println(read.count())
    val dirPath = "data/mllib/gmm_data.txt"
    val readFiles = sc. wholeTextFiles(dirPath)
    println("当前目录下总文件个数为：" + readFiles.count())

    sc.stop()
  }

  def sparkSession() = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("ReadLocalData")
      .getOrCreate()

    val path = "data/mllib/gmm_data.txt"
    val readFile = spark.sparkContext.textFile(path)
    println(readFile.count())

    val dirPath = "data/mllib/*/*"
    val readFiles = spark.sparkContext.wholeTextFiles(dirPath)
    println("当前目录下总文件个数为：" + readFiles.count())
    spark.close()
  }

  def main(args: Array[String]): Unit = {
    sparkContext()
    sparkSession()
  }
}
