/**
 * 
 */
package com.hdp.youtube.assignment.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import com.hdp.youtube.assignment.inputformat.YouTubeGenericWritable;
import com.hdp.youtube.assignment.record.RatedRecord;
import com.hdp.youtube.assignment.record.ViewedRecord;

/**
 * @author Naveen
 *
 */
public class YouTubeResultReducerGeneric extends Reducer<Text, YouTubeGenericWritable, Text, Text> {
	public void reduce(Text key, Iterable<YouTubeGenericWritable> values, Context context) throws IOException {
		try {

			for (YouTubeGenericWritable genericWritable : values) {
				Writable rawValue = genericWritable.get();
				if (rawValue instanceof ViewedRecord) {
					ViewedRecord viewedRecord = (ViewedRecord) rawValue;
					context.write(key, new Text(viewedRecord.toString()));
				}
				if (rawValue instanceof RatedRecord) {
					RatedRecord ratedRecord = (RatedRecord) rawValue;
					context.write(key, new Text(ratedRecord.toString()));
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
