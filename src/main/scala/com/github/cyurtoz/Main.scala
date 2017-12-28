package com.github.cyurtoz

import com.github.cyurtoz.learn.correlation.MoviesCorrelation
import com.github.cyurtoz.learn.regression.{MoviesLinearRegression, MoviesRegressionTree}
import com.github.cyurtoz.learn.{LearningParameters, Utils}
import com.github.cyurtoz.model.Movie
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import spray.json.DefaultJsonProtocol._
import spray.json._

/**
  * Created by cyurtoz on 10/12/17.
  *
  */
object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("cagatay").setMaster("local[7]")
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val lines = scala.io.Source.fromInputStream(Main.getClass.getResourceAsStream("/data.json")).mkString
    implicit val movieFormat = MovieJsonProtocol.MovieJsonFormat
    val movies: List[Movie] = lines.parseJson.convertTo[List[Movie]].filter(p => p != null)
    println("movies = " + movies.length)
    val moviesRDD = sc.parallelize(movies, 7)

    MoviesCorrelation.findCorrelations(moviesRDD)
    val (training, test) = Utils.splitTestAndTraining(movies = moviesRDD, 0.7f)
    val params = new LearningParameters(training, test,
      useGenres = true, useCountry = true, useYear = true)

    val linearRegression = new MoviesLinearRegression(params)
    val regTree = new MoviesRegressionTree(params)

    val dunkirk = Movie("Color", "Christopher Nolan", 525, 106, 322000, 6600, "", 2000, 187037289,
      " Action|Drama|History|Thriller|War", "", "Dunkirk", 227871, 10000, "", 1,
      "soldier|evacuation|army|military|rescure", "", 1297, "English", "US", "PG-13", 100000000, 2016, 0, 8.3, 2.20,
      639000)

    linearRegression.calculateTestMse()
    regTree.calculateTestMse()

    println("Regression Tree Prediction and result for "
      + dunkirk.movie_title + " " + regTree.predict(dunkirk, params))
    println("Linear Regression Prediction and result for "
      + dunkirk.movie_title + " " + linearRegression.predict(dunkirk, params))

    sc.stop()
  }

}
