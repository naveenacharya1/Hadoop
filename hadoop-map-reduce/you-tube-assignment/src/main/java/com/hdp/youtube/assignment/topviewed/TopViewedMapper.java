/**
 * 
 */
package com.hdp.youtube.assignment.topviewed;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Naveen
 *
 */
public class TopViewedMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException {
		try {
			String[] youTubeData = value.toString().split("\t");

			if (youTubeData == null || youTubeData.length <= 5)
				return;
			context.write(new Text(youTubeData[0]), new IntWritable(Integer.valueOf(youTubeData[5])));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
