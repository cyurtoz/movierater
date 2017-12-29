package com.github.cyurtoz.learn.regression

import com.github.cyurtoz.learn.{LearningParameters, Utils}
import com.github.cyurtoz.model.Movie
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Created by cyurtoz on 23/12/17.
  *
  */
abstract class LearningModel(val params: LearningParameters) extends Serializable {

  def calculateTestMse(): Unit = {
    println("Test MSE: "+ calculateMSE(params.testData))
  }

  def calculateTrainingMSE(): Unit = {
    println("Training MSE: "+ calculateMSE(params.trainingData))
  }

  def calculateMSE(data: RDD[Movie]): Double = {
    val labelsAndPredictions = data.map(predict(_, params))
    val testMSE = labelsAndPredictions.map { case (v, p) => math.pow(v - p, 2) }.mean()
    testMSE
  }

  def predict(movie:Movie, params:LearningParameters): (Double, Double)

}
