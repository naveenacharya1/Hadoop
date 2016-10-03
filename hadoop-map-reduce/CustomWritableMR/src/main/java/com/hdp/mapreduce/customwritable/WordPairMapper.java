/**
 * 
 */
package com.hdp.mapreduce.customwritable;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Naveen
 *
 */
public class WordPairMapper extends Mapper<LongWritable, Text, WordPair, IntWritable> {

	private Text secondWord = null;
	private WordPair wordPair = new WordPair();
	private static IntWritable one = new IntWritable(1);
	private Text firstWord = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException {
		try {
			String line = value.toString();
			line = line.replace(",", "");
			line = line.replace(".", "");

			for (String word : line.split("\\W+")) {
				if (secondWord == null) {
					secondWord = new Text(word);
				} else {
					firstWord.set(word);
					wordPair.set(secondWord, firstWord);
					context.write(wordPair, one);
					secondWord.set(firstWord.toString());
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
