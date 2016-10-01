/**
 * 
 */
package com.hdp.mapreduce.secondarysort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Partitioner;

/**
 * @author Naveen
 *
 */
public class NaturalKeyPartitioner extends MapReduceBase implements Partitioner<TemperatureKey, IntWritable> {

	public int getPartition(TemperatureKey key, IntWritable value, int numPartitions) {

		int hash = key.getYearMonth().hashCode();
		int partition = hash % numPartitions;
		return partition;

	}

}
