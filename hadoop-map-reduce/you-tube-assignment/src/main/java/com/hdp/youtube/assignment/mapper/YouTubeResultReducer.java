/**
 * 
 */
package com.hdp.youtube.assignment.mapper;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * @author Naveen
 *
 */
public class YouTubeResultReducer extends Reducer<Text, Text, Text, Text> {

	MultipleOutputs<Text, Text> mos;
	private Map<Text, IntWritable> countMap = new HashMap<Text, IntWritable>();
	private Map<Text, FloatWritable> ratedMap = new HashMap<Text, FloatWritable>();

	@Override
	public void setup(Context context) {
		mos = new MultipleOutputs<Text, Text>(context);
	}

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException {
		try {

			Iterator<Text> valuesIt = values.iterator();
			while (valuesIt.hasNext()) {
				String[] outPut = valuesIt.next().toString().split(",");

				// context.write(key, valuesIt.next());
				if ("View".equalsIgnoreCase(outPut[1])) {
					// mos.write("mostViewed", key, new Text(outPut[0]));
					countMap.put(new Text(key), new IntWritable(Integer.valueOf(outPut[0])));
				}

				if ("rating".equalsIgnoreCase(outPut[1])) {
					ratedMap.put(new Text(key), new FloatWritable(Float.valueOf(outPut[0])));
					// mos.write("topRated", key, new Text(outPut[0]));
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		List<Map.Entry<Text, IntWritable>> viewList = new LinkedList<Map.Entry<Text, IntWritable>>(countMap.entrySet());
		Collections.sort(viewList, new Comparator<Map.Entry<Text, IntWritable>>() {
			public int compare(Map.Entry<Text, IntWritable> o1, Map.Entry<Text, IntWritable> o2) {
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});

		Map<Text, IntWritable> sortedMapViewCount = new LinkedHashMap<Text, IntWritable>();
		for (Map.Entry<Text, IntWritable> entry : viewList) {
			sortedMapViewCount.put(entry.getKey(), entry.getValue());
		}

		int counterView = 0;
		for (Text key : sortedMapViewCount.keySet()) {
			if (counterView++ == 100) {
				break;
			}
			mos.write("mostViewed", key, sortedMapViewCount.get(key));
		}

		List<Map.Entry<Text, FloatWritable>> list = new LinkedList<Map.Entry<Text, FloatWritable>>(ratedMap.entrySet());
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
			mos.write("topRated", key, sortedMap.get(key));
		}

		mos.close();
	}
}
