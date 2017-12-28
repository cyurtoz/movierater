package com.github.cyurtoz.learn

import com.github.cyurtoz.model.Movie
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.Predict
import org.apache.spark.rdd.RDD

/**
  * Created by cyurtoz on 16/10/17.
  *
  */

object Utils {

  def splitTestAndTraining(movies: RDD[Movie], splitRate: Float): (RDD[Movie], RDD[Movie]) = {
    require(splitRate > 0f && splitRate < 1f)
    val splitt = movies.randomSplit(Array(splitRate, 1 - splitRate))
    (splitt(0), splitt(1))
  }

  def discretizeYears(bins:Int, trainingData: RDD[Movie]): Array[(Int, Int)] = {
    val data = trainingData.map(_.title_year).distinct
    val collected = data.collect().sorted
    val grouped = collected.grouped(bins).map(x=>(x.head, x.last)).toArray
    grouped
  }

  def extractGenres(moviesRDD: RDD[Movie]): Array[String] = {
    moviesRDD.map(_.genres.split('|')).collect.flatten.distinct
  }

  def extractContentRatings(moviesRDD: RDD[Movie]): Array[String] = {
    moviesRDD.map(_.content_rating).collect.distinct
  }

  def extractCountries(moviesRDD: RDD[Movie]): Array[String] = {
    moviesRDD.map(_.country).collect.distinct
  }

  def findYear(director: String, moviesRDD: RDD[Movie]): Unit = {
    val directorYear = moviesRDD.filter(mv => mv.director_name.eq(director) && mv.title_year != 0)
    directorYear.map(_.title_year).mean()
  }

  def extractMovieFeaturesVector(mv: Movie): Array[Double] = {

    val arr = Array(
      mv.movie_facebook_likes,
      mv.gross,
      mv.num_voted_users,
      mv.director_facebook_likes,
      mv.duration,
      mv.num_user_for_reviews,
      mv.num_critic_for_reviews
    )
    arr
  }

  def createCountryVector(country: String, countries: Array[String]): Array[Double] = {
    countries.map(ctr => if(ctr.equals(country)) 1d else 0d)
  }

  def createGenreVector(mv:Movie, genres: Array[String]): Array[Double] = {
    genres.map(x => if (mv.genres.contains(x)) 1d else 0d)
  }

  def createYearVector(mv: Movie, years: Array[(Int, Int)]): Array[Double] = {
    years.map(x => if (x._1 < mv.title_year && mv.title_year < x._2) 1d else 0d)

  }

  def extractMovieFeatures(mv: Movie, parameters: LearningParameters): LabeledPoint = {
    var core = Utils.extractMovieFeaturesVector(mv)

    if(parameters.useCountry) {
     core = core ++ createCountryVector(mv.country, parameters.countries)
    }

    if(parameters.useGenres) {
      core = core ++ createGenreVector(mv, parameters.genres)
    }

    if(parameters.useYear) {
      core = core ++ createYearVector(mv, parameters.years)
    }

    LabeledPoint(mv.imdb_score, Vectors.dense(core))
  }

}
