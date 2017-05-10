package com.michol.spark;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.ClassificationModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

import com.michol.spark.classifier.Detector;
import com.michol.spark.classifier.DetectorFactory;

import scala.Tuple2;

public class DetectorEngine {

	private static final int SPAM = 1;
	private static final int HAM = 0;
	private static HashingTF tf = new HashingTF(200);

	static Function<Tuple2<String, String>, LabeledPoint> labeledMails = new Function<Tuple2<String, String>, LabeledPoint>() {
		private static final long serialVersionUID = 6761286792641889484L;

		public LabeledPoint call(Tuple2<String, String> arg0) throws Exception {
			if (arg0._1().contains("spm")) {
				return new LabeledPoint(SPAM, tf.transform(Arrays.asList(arg0._2().split(" "))));
			} else {
				return new LabeledPoint(HAM, tf.transform(Arrays.asList(arg0._2().split(" "))));
			}
		}
	};

	public static ClassificationModel getTrainedModel(JavaPairRDD<String, String> emails, int classifier) {
		Detector detector = DetectorFactory.getDetector(classifier);
		return detector.getTrainedModel(createTraningData(emails));
	}

	private static JavaRDD<LabeledPoint> createTraningData(JavaPairRDD<String, String> emails) {
		JavaRDD<LabeledPoint> trainingData = emails.map(labeledMails);
		return trainingData.cache();
	}

	public static String detectSpam(String mail, ClassificationModel model) {
		Vector testExample = tf.transform(Arrays.asList(mail.split(" ")));
		double result = model.predict(testExample);
		return result == 1.0 ? "SPAM" : "HAM";
	}
}
