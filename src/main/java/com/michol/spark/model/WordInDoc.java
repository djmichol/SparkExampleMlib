package com.michol.spark.model;

import java.io.Serializable;

public class WordInDoc implements Serializable {

	private static final long serialVersionUID = 5628760502406034125L;

	public WordInDoc() {
	};

	public WordInDoc(String doc, String word, Integer count) {
		super();
		this.doc = doc;
		this.word = word;
		this.count = count;
	}

	String doc;
	String word;
	Integer count;

	public String getDoc() {
		return doc;
	}

	public void setDoc(String doc) {
		this.doc = doc;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return "Document [doc=" + doc + ", word=" + word + ", count=" + count + "]";
	}

}
