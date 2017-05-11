package com.michol.spark.classifier;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.ClassificationModel;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;

public class BayesDetector implements Detector {

	public ClassificationModel getTrainedModel(JavaRDD<LabeledPoint> trainingData) {
		JavaRDD<LabeledPoint>[] data = trainingData.randomSplit(new double[] { 0.7, 0.3 });
		JavaRDD<LabeledPoint> trainingSet = data[0];
		JavaRDD<LabeledPoint> testSet = data[1];
		NaiveBayesModel model = trainModel(trainingSet);
		printAccuracy(testSet, model);
		return model;
	}

	private NaiveBayesModel trainModel(JavaRDD<LabeledPoint> training) {
		NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);
		return model;
	}

	private void printAccuracy(JavaRDD<LabeledPoint> test, NaiveBayesModel model) {
		JavaPairRDD<Double, Double> predictionAndLabel = test.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
		double accuracy = predictionAndLabel.filter(pl -> pl._1().equals(pl._2())).count() / (double) test.count();
		System.out.println("Naive bayes accuracy: " + accuracy);
	}

}
