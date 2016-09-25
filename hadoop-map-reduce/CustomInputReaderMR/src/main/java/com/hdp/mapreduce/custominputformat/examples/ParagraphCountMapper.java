/**
 * 
 */
package com.hdp.mapreduce.custominputformat.examples;

/**
 * @author Naveen
 *
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ParagraphCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> collector, Reporter reporter)
			throws IOException {
		int numberOfChar = value.getLength();
		System.out.println("key: " + key);
		System.out.println("value: " + value);
		collector.collect(value, new IntWritable(numberOfChar));
	}
}