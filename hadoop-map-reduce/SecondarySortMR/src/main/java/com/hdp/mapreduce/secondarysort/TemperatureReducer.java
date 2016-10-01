/**
 * 
 */
package com.hdp.mapreduce.secondarysort;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author Naveen
 *
 */
public class TemperatureReducer extends MapReduceBase
		implements Reducer<TemperatureKey, IntWritable, TemperatureKey, Text> {
	StringBuffer stringBuffer = new StringBuffer();

	public void reduce(TemperatureKey key, Iterator<IntWritable> values, OutputCollector<TemperatureKey, Text> output,
			Reporter reporter) throws IOException {
		try {
			stringBuffer.append("[");
			while (values.hasNext()) {
				stringBuffer.append(values.next()).append(",");
			}
			String str = stringBuffer.toString().substring(0, stringBuffer.toString().length() - 1);
			stringBuffer.setLength(0);
			stringBuffer.append(str);
			stringBuffer.append("]");
			output.collect(key, new Text(stringBuffer.toString()));
			stringBuffer.setLength(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
