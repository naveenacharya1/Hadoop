/**
 * 
 */
package com.hdp.youtube.assignment.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hdp.youtube.assignment.record.ViewedRecord;

/**
 * @author Naveen
 *
 */
public class TopViewedMapper extends Mapper<Text, ViewedRecord, Text, Text> {

	public void map(Text key, ViewedRecord value, Context context) throws IOException {
		try {
			context.write(key, new Text(value.getViewCount() + "," + value.getRecordType()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
