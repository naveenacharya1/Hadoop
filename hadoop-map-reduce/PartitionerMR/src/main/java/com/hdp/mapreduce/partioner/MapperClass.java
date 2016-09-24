package com.hdp.mapreduce.partioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) {

		try {
			String[] inputData = value.toString().split("\t", -3);
			String gender = inputData[3];
			context.write(new Text(gender), new Text(value));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
