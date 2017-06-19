package com.michol.spark.bow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.michol.spark.model.BOWElement;
import com.michol.spark.model.WordInDoc;

import scala.Tuple2;

public class BagOfWord {
	static JavaSparkContext sc;

	static Function<JavaRDD<String>, JavaPairRDD<String, Integer>> mapWordWithCount = new Function<JavaRDD<String>, JavaPairRDD<String, Integer>>() {
		private static final long serialVersionUID = -3492703221767189060L;

		@Override
		public JavaPairRDD<String, Integer> call(JavaRDD<String> v1) throws Exception {
			return v1.mapToPair(word -> new Tuple2<>(word, 1));
		}
	};

	static Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>> sumWords = new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
		private static final long serialVersionUID = -7087440318784424805L;

		@Override
		public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> v1) throws Exception {
			return v1.reduceByKey((x, y) -> x + y);
		}
	};

	public static void main(String[] args) throws Exception {

		SparkConf conf = new SparkConf().setAppName("SortedWordCountSolution").setMaster("local[3]");
		sc = new JavaSparkContext(conf);
		BagOfWord bow = new BagOfWord();
		bow.runBow("src/main/resources");
		sc.stop();
	}

	public void runBow(String path) {
		// load all files in directory
		JavaPairRDD<String, String> files = sc.wholeTextFiles(path);
		runBow(files);
	}

	public void runBow(JavaPairRDD<String, String> files) {
		// map values per file to words list
		JavaPairRDD<String, JavaRDD<String>> wordsPerFile = files.mapValues(file -> {
			String fileWithoutNewLine = file.replace("\n", " ");
			String[] words = fileWithoutNewLine.split(" ");
			return sc.parallelize(Arrays.asList(words));
		});
		// map words with occurencies
		JavaPairRDD<String, JavaPairRDD<String, Integer>> wordsPerFileCount = wordsPerFile.mapValues(mapWordWithCount);
		// sum same word
		JavaPairRDD<String, JavaPairRDD<String, Integer>> sumWordPerFile = wordsPerFileCount.mapValues(sumWords);
		// map elements to single rdd
		JavaRDD<WordInDoc> wordsPerDoc = sumWordPerFile.flatMap(wordsInFile -> {
			List<WordInDoc> doc = new ArrayList<>();
			for (Tuple2<String, Integer> word : wordsInFile._2.collect()) {
				doc.add(new WordInDoc(wordsInFile._1, word._1(), word._2()));
			}
			return doc;
		});

		// group words in all doc by word
		JavaPairRDD<String, Iterable<WordInDoc>> groupedWords = wordsPerDoc.groupBy(word -> word.getWord());
		// map to bow element
		JavaRDD<BOWElement> bowElements = groupedWords.map(bowElem -> {
			Map<String, Integer> words = new HashMap<>();
			for (WordInDoc doc : bowElem._2) {
				words.put(doc.getDoc(), doc.getCount());
			}
			BOWElement bowElement = new BOWElement(bowElem._1, words);
			return bowElement;
		});
		// sort by word
		JavaRDD<BOWElement> bowElementsSorted = bowElements.sortBy(bowElem -> bowElem.getWord(), true, 1);
		// print all elements
		for (BOWElement bowElement : bowElementsSorted.collect()) {
			System.out.println(bowElement);
		}

	}
}
