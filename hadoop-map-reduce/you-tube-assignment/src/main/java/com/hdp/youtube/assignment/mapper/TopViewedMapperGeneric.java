/**
 * 
 */
package com.hdp.youtube.assignment.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hdp.youtube.assignment.inputformat.YouTubeGenericWritable;
import com.hdp.youtube.assignment.record.ViewedRecord;

/**
 * @author Naveen
 *
 */
public class TopViewedMapperGeneric extends Mapper<Text, ViewedRecord, Text, YouTubeGenericWritable> {

	public void map(Text key, ViewedRecord value, Context context) throws IOException {
		try {
			context.write(key, new YouTubeGenericWritable(value));
		} catch (Exception e) {
			System.out.println("ECEPTION------2");
			e.printStackTrace();
		}
	}

}
