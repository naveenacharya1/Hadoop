/**
 * 
 */
package com.hdp.youtube.assignment.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hdp.youtube.assignment.record.RatedRecord;

/**
 * @author Naveen
 *
 */
public class TopRatedMapper extends Mapper<Text, RatedRecord, Text, Text> {
	public void map(Text key, RatedRecord value, Context context) throws IOException {
		try {
			context.write(key, new Text(value.getRating()+","+value.getRecordType()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
