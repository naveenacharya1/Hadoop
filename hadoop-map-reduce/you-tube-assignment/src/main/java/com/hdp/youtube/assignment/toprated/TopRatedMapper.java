/**
 * 
 */
package com.hdp.youtube.assignment.toprated;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Naveen
 *
 */
public class TopRatedMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException {
		try {
			String[] youTubeData = value.toString().split("\t");

			if (youTubeData == null || youTubeData.length <= 7)
				return;
			context.write(new Text(youTubeData[0]), new FloatWritable(Float.valueOf(youTubeData[6])));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
