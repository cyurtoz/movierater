package com.github.cyurtoz.learn.correlation

import com.github.cyurtoz.learn.Utils
import com.github.cyurtoz.model.Movie
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

/**
  * Created by cyurtoz on 17/12/17.
  *
  */
object MoviesCorrelation {

  def findCorrelations(movies: RDD[Movie]): Unit = {
    val corrows = extractCorrelationRows(movies, Utils.extractGenres(movies))

    val correlMatrix = Statistics.corr(corrows, "pearson")
    val correlMatrix2 = Statistics.corr(corrows, "spearman")

    println("Pearson Correlation")
    printCorrelation(correlMatrix)
    println("Spearman Correlation")
    printCorrelation(correlMatrix2)

  }

  private def extractCorrelationRows(movies: RDD[Movie], genres: Array[String]) = {
    movies.map(mv => Vectors.dense(
      mv.imdb_score,
      mv.movie_facebook_likes,
      mv.gross,
      mv.num_voted_users,
      mv.actor_1_facebook_likes,
      mv.actor_2_facebook_likes,
      mv.actor_3_facebook_likes,
      mv.director_facebook_likes,
      mv.cast_total_facebook_likes,
      mv.duration,
      mv.num_user_for_reviews,
      mv.num_critic_for_reviews,
      mv.title_year,
      mv.budget,
      mv.facenumber_in_poster))
  }

  private def printCorrelation(correlMatrix: org.apache.spark.mllib.linalg.Matrix): Unit = {
    for (elem <- correlMatrix.rowIter) {
      elem.toArray.foreach(x => print(f"$x%1.3f "))
      print("\n")
    }
  }


}
