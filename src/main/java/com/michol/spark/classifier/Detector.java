package com.michol.spark.classifier;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.ClassificationModel;
import org.apache.spark.mllib.regression.LabeledPoint;

public interface Detector {

	ClassificationModel getTrainedModel(JavaRDD<LabeledPoint> trainingData);
	
}
