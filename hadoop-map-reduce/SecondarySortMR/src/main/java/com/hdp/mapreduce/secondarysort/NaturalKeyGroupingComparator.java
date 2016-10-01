/**
 * 
 */
package com.hdp.mapreduce.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/**
 * @author Naveen
 *
 */
public class NaturalKeyGroupingComparator extends WritableComparator {
	protected NaturalKeyGroupingComparator() {
		super(TemperatureKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		TemperatureKey k1 = (TemperatureKey) w1;
		TemperatureKey k2 = (TemperatureKey) w2;
		return k1.getYearMonth().compareTo(k2.getYearMonth());
	}
}
