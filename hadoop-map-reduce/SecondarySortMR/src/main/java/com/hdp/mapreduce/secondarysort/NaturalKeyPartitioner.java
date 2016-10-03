/**
 * 
 */
package com.hdp.mapreduce.secondarysort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Naveen
 *
 */
public class NaturalKeyPartitioner extends Partitioner<TemperatureKey, IntWritable> {

	public int getPartition(TemperatureKey key, IntWritable value, int numPartitions) {
		int hash = key.getYearMonth().hashCode();
		int partition = hash % numPartitions;
		return partition;
	}

}
