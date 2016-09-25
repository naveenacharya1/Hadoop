/**
 * 
 */
package com.hdp.mapreduce.custominputformat.examples;

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

public class ParagraphCount {

	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		Path inputPath = new Path("hdfs://127.0.0.1:9000/input/paragraphinput.txt");
		Path outputPath = new Path("hdfs://127.0.0.1:9000/output/result");

		FileSystem hdfs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), config);
		if (hdfs.exists(outputPath))
			hdfs.delete(outputPath, true);

		JobConf conf = new JobConf(config, ParagraphCount.class);

		conf.setInputFormat(ParagrapghInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setMapperClass(ParagraphCountMapper.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);

		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);

		RunningJob runningJob = JobClient.runJob(conf);
		System.out.println("Job Successfull: " + runningJob.isComplete());
	}

}
