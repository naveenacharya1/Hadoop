/**
 * 
 */
package com.hdp.mapreduce.customwritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author Naveen
 *
 */
public class WordPair implements WritableComparable<WordPair> {

	private Text firstWord;
	private Text secondWord;

	public WordPair() {
		set(new Text(), new Text());
	}

	public WordPair(Text firstWord, Text secondWord) {
		set(firstWord, secondWord);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((firstWord == null) ? 0 : firstWord.hashCode());
		result = prime * result + ((secondWord == null) ? 0 : secondWord.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof WordPair) {
			WordPair wordPair = (WordPair) obj;
			return firstWord.equals(wordPair.firstWord) && secondWord.equals(wordPair.secondWord);
		}
		return false;
	}

	public void readFields(DataInput input) throws IOException {
		firstWord.readFields(input);
		secondWord.readFields(input);

	}

	public void write(DataOutput output) throws IOException {
		firstWord.write(output);
		secondWord.write(output);

	}

	public int compareTo(WordPair wp) {
		int cmp = firstWord.compareTo(wp.firstWord);
		if (cmp != 0) {
			return cmp;
		}
		return secondWord.compareTo(wp.secondWord);
	}

	public void set(Text firstWord, Text secondWord) {
		this.firstWord = firstWord;
		this.secondWord = secondWord;
	}

	@Override
	public String toString() {
		return firstWord + " " + secondWord;
	}

}
