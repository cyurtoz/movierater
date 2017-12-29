package com.github.cyurtoz.learn.regression

import com.github.cyurtoz.learn.{LearningParameters, Utils}
import com.github.cyurtoz.model.Movie
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession

class RandomForestRegression(params:LearningParameters) extends LearningModel(params) {

//  val model: PipelineModel = {
//
//    val trainingDataFeatures = params.trainingData.map(Utils.extractMovieFeatures(_,params))
//    val testDataFeatures = params.testData.map(Utils.extractMovieFeatures(_,params))
//
//    val sparkSession =  SparkSession.builder().getOrCreate()
//    val pointsTrainDf =  sparkSession.createDataset(trainingDataFeatures)
//    val pointsTestDf =  sparkSession.createDataset(testDataFeatures)
//
//
//    val featureIndexer = new VectorIndexer()
//      .setInputCol("features")
//      .setOutputCol("indexedFeatures")
//      .setMaxCategories(4)
//      .fit(pointsTrainDf)
//
//    val labelIndexer = new StringIndexer()
//      .setInputCol("label")
//      .setOutputCol("indexedLabel")
//      .fit(pointsTrainDf)
//
//    val rf = new RandomForestClassifier()
//      .setLabelCol("indexedLabel")
//      .setFeaturesCol("indexedFeatures")
//      .setNumTrees(10)
//
//    // Convert indexed labels back to original labels.
//    val labelConverter = new IndexToString()
//      .setInputCol("prediction")
//      .setOutputCol("predictedLabel")
//      .setLabels(labelIndexer.labels)
//
//    // Chain indexers and forest in a Pipeline.
//    val pipeline = new Pipeline()
//      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))
//
//
//    // Train model. This also runs the indexer.
//   val model =  pipeline.fit(pointsTrainDf)
//    val predictions = model.transform(pointsTestDf)
//
//    predictions.select("predictedLabel", "label", "features").show(5)
//
//    // Select (prediction, true label) and compute test error.
//    val evaluator = new MulticlassClassificationEvaluator()
//      .setLabelCol("indexedLabel")
//      .setPredictionCol("prediction")
//      .setMetricName("accuracy")
//    val accuracy = evaluator.evaluate(predictions)
//    println("Test Error = " + (1.0 - accuracy))
//
//    val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
//    println("Learned classification forest model:\n" + rfModel.toDebugString)
//
//    model
//  }

  override def predict(movie: Movie, params: LearningParameters): (Double, Double) = {

    (1,1)


  }
}
