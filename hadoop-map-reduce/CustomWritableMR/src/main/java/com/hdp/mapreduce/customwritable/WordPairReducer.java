/**
 * 
 */
package com.hdp.mapreduce.customwritable;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Naveen
 *
 */
public class WordPairReducer extends Reducer<WordPair, IntWritable, WordPair, IntWritable> {

	public void reduce(WordPair key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		Iterator<IntWritable> valuesIt = values.iterator();
		while (valuesIt.hasNext()) {
			sum = sum + valuesIt.next().get();
		}
		context.write(key, new IntWritable(sum));
	}

}
