/**
 * 
 */
package com.hdp.mapreduce.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author Naveen
 *
 */
public class TemperatureKey implements WritableComparable<TemperatureKey> {

	private IntWritable yearMonth;
	private IntWritable temperature;

	public TemperatureKey() {
		set(new IntWritable(), new IntWritable());
	}

	public void set(IntWritable yearMonth, IntWritable temperature) {
		this.yearMonth = yearMonth;
		this.temperature = temperature;

	}

	public TemperatureKey(IntWritable yearMonth, IntWritable temperature) {
		set(yearMonth, temperature);
	}

	public void write(DataOutput out) throws IOException {
		yearMonth.write(out);
		temperature.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		yearMonth.readFields(in);
		temperature.readFields(in);
	}

	public int compareTo(TemperatureKey o) {
		return yearMonth.compareTo(o.yearMonth);
	}

	@Override
	public String toString() {
		return yearMonth.toString();
	}

}
