package com.hdp.mapreduce.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TemperatureMapper extends Mapper<LongWritable, Text, TemperatureKey, IntWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException {

		try {
			String[] tempData = value.toString().split(",");
			TemperatureKey temperatureKey = new TemperatureKey();
			String keyData = tempData[0] + tempData[1];
			temperatureKey.set(new IntWritable(Integer.valueOf(keyData)),
					new IntWritable(Integer.valueOf(tempData[2])));
			context.write(temperatureKey, new IntWritable(Integer.valueOf(tempData[2])));

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
