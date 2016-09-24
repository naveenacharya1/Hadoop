package com.hdp.mapreduce.partioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) {
		int max = 0;
		try {
			for (Text text : values) {
				String[] data = text.toString().split("\t");
				int sal = Integer.valueOf(data[4].substring(1, data[4].length()));
				max = sal > max ? sal : max;
			}
			context.write(new Text(key), new Text("$" + String.valueOf(max)));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
