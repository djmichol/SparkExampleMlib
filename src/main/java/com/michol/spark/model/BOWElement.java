package com.michol.spark.model;

import java.io.Serializable;
import java.util.Map;

public class BOWElement implements Serializable {

	private static final long serialVersionUID = 3345863920626698003L;

	public BOWElement() {
	}

	private String word;
	private Map<String, Integer> occurienses;

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public Map<String, Integer> getOccurienses() {
		return occurienses;
	}

	public void setOccurienses(Map<String, Integer> occurienses) {
		this.occurienses = occurienses;
	}

	public BOWElement(String word, Map<String, Integer> occurienses) {
		super();
		this.word = word;
		this.occurienses = occurienses;
	}

	@Override
	public String toString() {
		return "BOWElement [word=" + word + ", occurienses=" + occurienses + "]";
	}

	
}
