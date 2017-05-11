package com.michol.spark.classifier;

public class DetectorFactory {

	public static final int BAYES = 1;
	public static final int LOGISTIC_REGRESSION = 2;

	public static Detector getDetector(int detector){
		switch (detector) {
		case 1:
			return new BayesDetector();
		case 2:
			return new LogisticRegressionDetector();
		}
		return new LogisticRegressionDetector();
	}
}
