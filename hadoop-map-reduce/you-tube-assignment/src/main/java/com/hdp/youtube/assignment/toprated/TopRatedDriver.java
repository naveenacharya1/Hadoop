package com.hdp.youtube.assignment.toprated;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TopRatedDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Path inputPath = new Path("hdfs://127.0.0.1:9000/input/youtubedata.txt");
		Path outputPath = new Path("hdfs://127.0.0.1:9000/output/assignment/youtube/toprated");

		Job job = Job.getInstance();
		job.setJarByClass(TopRatedDriver.class);
		job.setJobName("TopCategoryDriver");

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(TopRatedMapper.class);
		job.setReducerClass(TopRatedReducer.class);
		FileSystem hdfs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), conf);
		if (hdfs.exists(outputPath))
			hdfs.delete(outputPath, true);

		int returnValue = job.waitForCompletion(true) ? 0 : 1;
		System.out.println("job.isSuccessful " + job.isSuccessful());
		System.exit(returnValue);
	}

}
