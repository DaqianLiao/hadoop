package com.ldq.study.base

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession

object LRFilterCV {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Bayes")
      .master("local")
      .getOrCreate()

    val initData = spark.createDataFrame(Seq(
      (1, 1, "hope you are well"),
      (2, 1, "nice to hear from you"),
      (3, 1, "happy weekend"),
      (4, 1, "go to school"),
      (5, 1, "swimming in the oceans"),
      (6, 1, "rain rain go away"),
      (7, 1, "play with muxi"),
      (8, 1, "learn java"),
      (9, 1, "leetcode practice"),
      (0, 1, "happy weekend"),
      (10, 0, "fuck you"),
      (11, 0, "i need money"),
      (12, 0, "stolen money"),
      (13, 0, "i am hungry"),
      (14, 0, "hate others"),
      (15, 0, "i have no money"),
      (16, 0, "no brother"),
      (17, 0, "stay alone"),
      (18, 0, "save money"),
      (19, 0, "stupid dog")
    )).toDF("userid", "label", "post")

    initData.show(false)
    val Array(train, test) = initData.randomSplit(Array(0.8, 0.2))

    val tokenizer = new Tokenizer().setInputCol("post").setOutputCol("words")
    val hashTF = new HashingTF().setInputCol("words").setOutputCol("tf")
    val lr = new LogisticRegression().setFeaturesCol("tf")

    val pipeline = new Pipeline().setStages(Array(tokenizer, hashTF, lr))

    val paramMap = new ParamGridBuilder()
      .addGrid(hashTF.numFeatures, Array(100, 200))
      .addGrid(lr.regParam, Array(0.02, 0.05))
      .addGrid(lr.maxIter, Array(3, 5))
      .build()

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEstimatorParamMaps(paramMap)
      .setEvaluator(evaluator)
      .setNumFolds(5)


    val crossValidatorModel = cv.fit(train)
    val prediction = crossValidatorModel.transform(test)
    val accuracy = evaluator.evaluate(prediction)
    println("accuracy = " + accuracy)

    val pipelineModel = crossValidatorModel.bestModel.asInstanceOf[PipelineModel]
    pipelineModel.stages.toList.foreach(x => println(x.extractParamMap()))

    //    val model = pipeline.fit(initData)

    //    val test = spark.createDataFrame(Seq(
    //      (19, 1, "hope you are well"),
    //      (18, 0, "fuck god"),
    //      (15, 1, "new sun drive car"),
    //      (16, 0, "stupid girl come in")
    //    )).toDF("userid", "label", "post")
    //
    //        val prediction = model.transform(test)
    //        prediction.show(false)
    //
    //    val evaluator = new MulticlassClassificationEvaluator()
    //      .setLabelCol("label")
    //      .setPredictionCol("prediction")
    //      .setMetricName("accuracy")
    //
    //    val accuracy = evaluator.evaluate(prediction)
    //    println("Test set accuracy = ", accuracy)
  }

}
