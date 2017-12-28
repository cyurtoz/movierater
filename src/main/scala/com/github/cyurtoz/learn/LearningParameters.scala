package com.github.cyurtoz.learn

import com.github.cyurtoz.model.Movie
import org.apache.spark.rdd.RDD

/**
  * Created by cyurtoz on 20/12/17.
  *
  */
class LearningParameters (
                              val trainingData: RDD[Movie],
                              val testData: RDD[Movie],
                              val useGenres: Boolean,
                              val useCountry: Boolean,
                              val useYear: Boolean
                              ) extends Serializable {

  val countries: Array[String] = {
    if (useCountry)
      Utils.extractCountries(trainingData)
    else Array.empty
  }

  val genres: Array[String] = {
    if (useGenres)
      Utils.extractGenres(trainingData)
    else Array.empty
  }

  val years: Array[(Int, Int)] = {
    if (useYear)
      Utils.discretizeYears(10, trainingData)
    else Array.empty
  }

}

