/**
 * 
 */
package com.hdp.mapreduce.customwritable;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author Naveen
 *
 */
public class WordPairReducer extends MapReduceBase implements Reducer<WordPair, IntWritable, WordPair, IntWritable> {

	public void reduce(WordPair key, Iterator<IntWritable> values, OutputCollector<WordPair, IntWritable> output,
			Reporter reporter) throws IOException {
		int sum = 0;
		while (values.hasNext()) {
			sum = sum + values.next().get();
		}
		output.collect(key, new IntWritable(sum));
	}

}
