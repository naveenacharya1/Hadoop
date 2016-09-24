/**
 * 
 */
package com.hdp.mapreduce.partioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Naveen
 *
 */
public class PartitionerClass extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int numberOfReduceTask) {

		String[] str = value.toString().split("\t");
		int age = Integer.parseInt(str[2]);
		if (numberOfReduceTask == 0)
			return 0;
		// send data to first reducer
		if (age <= 20) {
			return 0;
		} else if (age > 20 && age <= 30) {
			return 1 % numberOfReduceTask;// send data to second reducer
		} else {
			return 2 % numberOfReduceTask; // send data to third reducer
		}

	}

}
