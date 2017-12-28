package com.github.cyurtoz.learn.regression

import com.github.cyurtoz.learn.{LearningParameters, Utils}
import com.github.cyurtoz.model.Movie
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD

/**
  * Created by cyurtoz on 13/12/17.
  *
  */
class MoviesLinearRegression(params:LearningParameters) extends LearningModel(params) {

  val model: LinearRegressionModel = {
    val numIterations = 80
    val stepSize = 0.000000000000000001

    val genres = Utils.extractGenres(params.trainingData)

    var features = params.trainingData.map(Utils.extractMovieFeatures(_, params))

    LinearRegressionWithSGD.train(features, numIterations, stepSize)

  }

  override def predict(movie:Movie, params:LearningParameters): (Double, Double) = {
    val result = model.predict(Utils.extractMovieFeatures(movie, params).features)
    (movie.imdb_score, result)
  }
}
