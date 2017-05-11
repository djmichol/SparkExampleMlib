package com.michol.spark;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.ClassificationModel;

import com.michol.spark.classifier.DetectorFactory;

public class SpamDetector {

	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf().setAppName("org.sparkexample.WordCount").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		runDetector(sc);
	}

	public static void runDetector(JavaSparkContext sc) throws Exception {
		JavaPairRDD<String, String> emails = sc.wholeTextFiles("files/all/*.txt");
		ClassificationModel model = DetectorEngine.getTrainedModel(emails, DetectorFactory.BAYES);
		String spam = IOUtils.toString(SpamDetector.class.getClassLoader().getResourceAsStream("spam.txt"));
		String ham = IOUtils.toString(SpamDetector.class.getClassLoader().getResourceAsStream("ham.txt"));
		System.out.println("Prediction for spam test example: " + DetectorEngine.detectSpam(spam, model));
		System.out.println("Prediction for ham test example: " + DetectorEngine.detectSpam(ham, model));
		sc.stop();
	}
}
