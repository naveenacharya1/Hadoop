package com.hdp.mapreduce.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TemperatureMapper extends MapReduceBase
		implements Mapper<LongWritable, Text, TemperatureKey, IntWritable> {

	public void map(LongWritable key, Text value, OutputCollector<TemperatureKey, IntWritable> output,
			Reporter reporter) throws IOException {

		try {
			String[] tempData = value.toString().split(",");
			TemperatureKey temperatureKey = new TemperatureKey();
			String keyData = tempData[0] + tempData[1];
			temperatureKey.set(new IntWritable(Integer.valueOf(keyData)),
					new IntWritable(Integer.valueOf(tempData[2])));
			output.collect(temperatureKey, new IntWritable(Integer.valueOf(tempData[2])));

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
