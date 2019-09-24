package com.ldq.study.base

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

object VectorGenerate {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Bayes")
      .master("local")
      .getOrCreate()

    val training = spark.createDataFrame(Seq(
      ("Lily", 85, 90,1),
      ("jack", 90, 70,0),
      ("emily", 80, 99,1),
      ("Emily", 80, 49,0),
      ("leo", 100, 100,1))
    ).toDF("userid", "enScore", "cnScore","label")

    /**
     * 将多个数值数据组装成向量，
     * 指定列名，组成新向量列
     */
    val assembler = new VectorAssembler()
      .setInputCols(Array("enScore", "cnScore"))
      .setOutputCol("feature")
    val frame = assembler.transform(training)
    frame.show(false)

    val lr = new LogisticRegression()
      .setRegParam(0.01)
      .setMaxIter(10)
      .setLabelCol("label")
      .setFeaturesCol("feature")

    val model = lr.fit(frame)
    model.transform(frame).show(false)
  }
}
