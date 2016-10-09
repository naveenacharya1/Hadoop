/**
 * 
 */
package com.hdp.youtube.assignment.second;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.hdp.youtube.assignment.inputformat.RatedInputFormat;
import com.hdp.youtube.assignment.inputformat.ViewInputFormat;
import com.hdp.youtube.assignment.mapper.TopRatedMapper;
import com.hdp.youtube.assignment.mapper.TopViewedMapper;
import com.hdp.youtube.assignment.mapper.YouTubeResultReducer;

/**
 * @author Naveen
 *
 */
public class YouTubeJobDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Path viewCountPath = new Path("hdfs://127.0.0.1:9000/input/youtubedata_viewcount.txt");
		Path ratingPath = new Path("hdfs://127.0.0.1:9000/input/youtubedata_rating.txt");
		Path outputPath = new Path("hdfs://127.0.0.1:9000/output/assignment/youtube/topviewed");

		Job job = Job.getInstance();
		job.setJarByClass(YouTubeJobDriver.class);
		job.setJobName("YouTubeJobDriver");

		FileOutputFormat.setOutputPath(job, outputPath);

		// mapper output format
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// reducer output format
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, viewCountPath, ViewInputFormat.class, TopViewedMapper.class);
		MultipleInputs.addInputPath(job, ratingPath, RatedInputFormat.class, TopRatedMapper.class);
		job.setReducerClass(YouTubeResultReducer.class);

		MultipleOutputs.addNamedOutput(job, "mostViewed", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "topRated", TextOutputFormat.class, Text.class, Text.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		FileSystem hdfs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), conf);
		if (hdfs.exists(outputPath))
			hdfs.delete(outputPath, true);

		int returnValue = job.waitForCompletion(true) ? 0 : 1;
		System.out.println("job.isSuccessful " + job.isSuccessful());
		System.exit(returnValue);
	}
}
