import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.ClassificationModel;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.michol.spark.DetectorEngine;
import com.michol.spark.classifier.DetectorFactory;

public class SpamTest {

	private JavaSparkContext sc;
	private SparkConf sparkConf;

	@Before
	public void setUp() {
		sparkConf = new SparkConf().setAppName("org.sparkexample.WordCount").setMaster("local");
		sc = new JavaSparkContext(sparkConf);
	}

	@Test
	public void getTrainedBayesModelNotNull() {
		JavaPairRDD<String, String> emails = sc.wholeTextFiles("src/test/resources/emails/*.txt");
		ClassificationModel model = DetectorEngine.getTrainedModel(emails, DetectorFactory.BAYES);
		Assert.assertNotNull(model);
	}
	
	@Test
	public void getTrainedLinearModelNotNull() {
		JavaPairRDD<String, String> emails = sc.wholeTextFiles("src/test/resources/emails/*.txt");
		ClassificationModel model = DetectorEngine.getTrainedModel(emails, DetectorFactory.LOGISTIC_REGRESSION);
		Assert.assertNotNull(model);
	}
	
	@Test(expected = NullPointerException.class) 
	public void getTrainedBayesModelNullEmails() {
		JavaPairRDD<String, String> emails = null;
		DetectorEngine.getTrainedModel(emails, DetectorFactory.BAYES);
	}
	
	@Test(expected = NullPointerException.class) 
	public void getTrainedLinearRegModelNullEmails() {
		JavaPairRDD<String, String> emails = null;
		DetectorEngine.getTrainedModel(emails, DetectorFactory.LOGISTIC_REGRESSION);
	}
	
	@Test
	public void predictLinearRegSpamMailExeptedSpam() throws IOException {
		LogisticRegressionModel model = LogisticRegressionModel.load(JavaSparkContext.toSparkContext(sc), "src/test/resources/model");
		String mail = IOUtils.toString(SpamTest.class.getClassLoader().getResourceAsStream("spam.txt"));
		String result = DetectorEngine.detectSpam(mail, model);
		Assert.assertEquals("SPAM", result);
	}
	
	@Test
	public void predictLinearRegHamMailExeptedHam() throws IOException {
		LogisticRegressionModel model = LogisticRegressionModel.load(JavaSparkContext.toSparkContext(sc), "src/test/resources/model");
		String mail = IOUtils.toString(SpamTest.class.getClassLoader().getResourceAsStream("ham.txt"));
		String result = DetectorEngine.detectSpam(mail, model);
		Assert.assertEquals("HAM", result);
	}
	
	@Test
	public void predictBayesSpamMailExeptedSpam() throws IOException {
		ClassificationModel model = NaiveBayesModel.load(JavaSparkContext.toSparkContext(sc), "src/test/resources/naiveBayesModel");
		String mail = IOUtils.toString(SpamTest.class.getClassLoader().getResourceAsStream("spam.txt"));
		String result = DetectorEngine.detectSpam(mail, model);
		Assert.assertEquals("SPAM", result);
	}
	
	@Test
	public void predictBayesHamMailExeptedHam() throws IOException {
		ClassificationModel model = NaiveBayesModel.load(JavaSparkContext.toSparkContext(sc), "src/test/resources/naiveBayesModel");
		String mail = IOUtils.toString(SpamTest.class.getClassLoader().getResourceAsStream("ham.txt"));
		String result = DetectorEngine.detectSpam(mail, model);
		Assert.assertEquals("HAM", result);
	}
	
	@After
	public void tearDown(){
		if(sc!=null){
			sc.stop();
		}
	}

}
