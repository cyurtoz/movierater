package com.github.cyurtoz.learn.regression

/**
  * Created by cyurtoz on 15/12/17.
  *
  */

import com.github.cyurtoz.learn.{LearningParameters, Utils}
import com.github.cyurtoz.model.Movie
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel

class MoviesRegressionTree(params:LearningParameters) extends LearningModel(params) {

  val model: DecisionTreeModel = {
    val genres = Utils.extractGenres(params.trainingData)
    val countries = Utils.extractCountries(params.testData)

    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "variance"

    val trainingDataFeatures = params.trainingData.map(Utils.extractMovieFeatures(_,params))

    val maxDepth = 6
    val maxBins = 32

    DecisionTree.trainRegressor(trainingDataFeatures,
      categoricalFeaturesInfo, impurity, maxDepth, maxBins)

  }

  override def predict(movie:Movie, params:LearningParameters): (Double, Double) = {
    val result = model.predict(Utils.extractMovieFeatures(movie, params).features)
    (movie.imdb_score, result)
  }
}
