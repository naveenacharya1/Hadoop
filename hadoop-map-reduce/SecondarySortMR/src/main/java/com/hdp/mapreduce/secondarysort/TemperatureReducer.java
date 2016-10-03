package com.hdp.mapreduce.secondarysort;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TemperatureReducer extends Reducer<TemperatureKey, IntWritable, IntWritable, Text> {
	StringBuffer stringBuffer = new StringBuffer();

	public void reduce(TemperatureKey key, Iterable<IntWritable> values, Context context) throws IOException {
		try {

			stringBuffer.append("[");

			Iterator<IntWritable> valuesIt = values.iterator();
			while (valuesIt.hasNext()) {
				stringBuffer.append(valuesIt.next()).append(",");
			}
			stringBuffer.setLength(stringBuffer.length() - 1);
			stringBuffer.append("]");
			context.write(key.getYearMonth(), new Text(stringBuffer.toString()));
			System.out.println("Reducer Value Output :" + stringBuffer);
			stringBuffer.setLength(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
