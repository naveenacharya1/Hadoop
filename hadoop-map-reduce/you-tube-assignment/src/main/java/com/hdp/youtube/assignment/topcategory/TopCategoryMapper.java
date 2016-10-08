/**
 * 
 */
package com.hdp.youtube.assignment.topcategory;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Naveen
 *
 */
public class TopCategoryMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context) throws IOException {
		try {
			String[] youTubeData = value.toString().split("\t");

			if (youTubeData == null || youTubeData.length <= 3 || youTubeData[3] == null)
				return;
			context.write(new Text(youTubeData[3]), one);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
