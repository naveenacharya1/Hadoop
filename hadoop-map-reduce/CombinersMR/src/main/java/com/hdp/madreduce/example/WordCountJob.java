/**
 * 
 */
package com.hdp.madreduce.example;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountJob {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Path inputPath = new Path("hdfs://127.0.0.1:9000/input/wordcount.txt");
		Path outputPath = new Path("hdfs://127.0.0.1:9000/output/combiner/result");

		@SuppressWarnings("deprecation")
		Job job = new Job();
		job.setJarByClass(WordCountJob.class);
		job.setJobName("WordCounterJob");

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);

		FileSystem hdfs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), conf);
		if (hdfs.exists(outputPath))
			hdfs.delete(outputPath, true);

		int returnValue = job.waitForCompletion(true) ? 0 : 1;
		System.out.println("job.isSuccessful " + job.isSuccessful());
		System.exit(returnValue);
	}

}
