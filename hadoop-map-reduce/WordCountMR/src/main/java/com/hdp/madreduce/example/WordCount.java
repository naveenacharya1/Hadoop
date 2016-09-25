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
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WordCount {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Path inputPath = new Path("hdfs://127.0.0.1:9000/input/WordCountSample.txt");
		Path outputPath = new Path("hdfs://127.0.0.1:9000/output/result");

		JobConf job = new JobConf(conf, WordCount.class);
		job.setJarByClass(WordCount.class);
		job.setJobName("WordCounterJob");

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		FileSystem hdfs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), conf);
		if (hdfs.exists(outputPath))
			hdfs.delete(outputPath, true);

		RunningJob runningJob = JobClient.runJob(job);
		System.out.println("Job Successfull: " + runningJob.isComplete());
	}

}
