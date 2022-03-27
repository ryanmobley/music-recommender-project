/*
Music Recommender System - Model and Evaluation

Requires that preprocessor is run first; needs ProcessedData/users_songs.csv

If more memory is required...
> spark-shell --driver-memory 2g
*/

import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.evaluation.RegressionEvaluator

// Read in processed data
val df = spark.read.option("header", true).option("inferSchema", true).
    csv("ProcessedData/users_songs.csv")

// Randomly split to training (90%) and cross-validation (10%) sets
val Array(trainData, cvData) = df.randomSplit(Array(0.9, 0.1))
trainData.cache()
cvData.cache()

// Create and collaborative filtering model
val model = new ALS().
    //setImplicitPrefs(true).
    setRank(10).setRegParam(0.01).setAlpha(1.0).setMaxIter(5).
    setUserCol("user_id").
    setItemCol("song_id").
    setRatingCol("target").
    fit(trainData)

// Generate predictions for cv set
// Drop prediction row if no prediction can be made
model.setColdStartStrategy("drop")
val predictions = model.transform(cvData)

// RMSE: The average is to predict a target of 0.5 for each sample.
// In this case, the RMSE = 0.5.
// Values under 0.5 are good; values above are bad.
val evaluator = new RegressionEvaluator().
    setMetricName("rmse").
    setLabelCol("target").
    setPredictionCol("prediction")
val rmse = evaluator.evaluate(predictions)

// TODO: evaluation of model with varying hyperparameters
