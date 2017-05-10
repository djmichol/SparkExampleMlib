package com.michol.spark.classifier;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.ClassificationModel;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.regression.LabeledPoint;

public class LogisticRegressionDetector implements Detector{

	@Override
	public ClassificationModel getTrainedModel(JavaRDD<LabeledPoint> trainingData) {
		LogisticRegressionWithSGD lrLearner = new LogisticRegressionWithSGD();
		LogisticRegressionModel model = lrLearner.run(trainingData.rdd());
		return model;
	}

}
