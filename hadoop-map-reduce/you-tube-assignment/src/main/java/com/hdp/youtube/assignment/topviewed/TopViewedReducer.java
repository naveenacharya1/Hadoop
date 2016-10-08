/**
 * 
 */
package com.hdp.youtube.assignment.topviewed;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Naveen
 *
 */
public class TopViewedReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private Map<Text, IntWritable> countMap = new HashMap<Text, IntWritable>();

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException {
		try {

			Iterator<IntWritable> valuesIt = values.iterator();
			while (valuesIt.hasNext()) {
				countMap.put(new Text(key), new IntWritable(valuesIt.next().get()));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		List<Map.Entry<Text, IntWritable>> list = new LinkedList<Map.Entry<Text, IntWritable>>(countMap.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<Text, IntWritable>>() {
			public int compare(Map.Entry<Text, IntWritable> o1, Map.Entry<Text, IntWritable> o2) {
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});

		Map<Text, IntWritable> sortedMap = new LinkedHashMap<Text, IntWritable>();
		for (Map.Entry<Text, IntWritable> entry : list) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		int counter = 0;
		for (Text key : sortedMap.keySet()) {
			if (counter++ == 100) {
				break;
			}
			context.write(key, sortedMap.get(key));
		}
	}

}
