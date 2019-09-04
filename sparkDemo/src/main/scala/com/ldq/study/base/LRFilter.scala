package com.ldq.study.base

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.SparkSession

object LRFilter {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Bayes")
      .master("local")
      .getOrCreate()

    val training = spark.createDataFrame(Seq(
      (1, 1, "hope you are well"),
      (2, 1, "nice to hear from you"),
      (3, 1, "happy weekend"),
      (4, 0, "fuck you"),
      (5, 0, "save money"),
      (6, 0, "stupid dog")
    )).toDF("userid", "label", "post")

    training.show(false)
    val tokenizer = new Tokenizer().setInputCol("post").setOutputCol("words")
    val hashTF = new HashingTF().setNumFeatures(500).setInputCol("words").setOutputCol("tf")
    val lr =new LogisticRegression().setMaxIter(10).setRegParam(0.01).setFeaturesCol("tf")

    val pipeline = new Pipeline().setStages(Array(tokenizer, hashTF, lr))

    val model = pipeline.fit(training)

    val test = spark.createDataFrame(Seq(
      (19, 1, "hope you are well"),
      (18, 0, "fuck god"),
      (15, 1, "new sun drive car"),
      (16, 0, "stupid girl come in")
    )).toDF("userid", "label", "post")

    val prediction = model.transform(test)
    prediction.show(false)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(prediction)
    println("Test set accuracy = ", accuracy)
  }

}
