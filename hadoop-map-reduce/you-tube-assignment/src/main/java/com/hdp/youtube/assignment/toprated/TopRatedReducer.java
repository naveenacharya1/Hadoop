/**
 * 
 */
package com.hdp.youtube.assignment.toprated;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Naveen
 *
 */
public class TopRatedReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

	private Map<Text, FloatWritable> countMap = new HashMap<Text, FloatWritable>();

	public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException {
		try {
			
			Iterator<FloatWritable> valuesIt = values.iterator();
			while (valuesIt.hasNext()) {
				countMap.put(new Text(key), new FloatWritable(valuesIt.next().get()));
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		List<Map.Entry<Text, FloatWritable>> list = new LinkedList<Map.Entry<Text, FloatWritable>>(countMap.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<Text, FloatWritable>>() {
			public int compare(Map.Entry<Text, FloatWritable> o1, Map.Entry<Text, FloatWritable> o2) {
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});

		Map<Text, FloatWritable> sortedMap = new LinkedHashMap<Text, FloatWritable>();
		for (Map.Entry<Text, FloatWritable> entry : list) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		int counter = 0;
		for (Text key : sortedMap.keySet()) {
			if (counter++ == 10) {
				break;
			}
			context.write(key, sortedMap.get(key));
		}
	}

}
