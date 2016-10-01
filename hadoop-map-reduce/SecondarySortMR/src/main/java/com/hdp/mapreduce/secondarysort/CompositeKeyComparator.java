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
public class CompositeKeyComparator extends WritableComparator {

	protected CompositeKeyComparator() {
		super(TemperatureKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		TemperatureKey k1 = (TemperatureKey) w1;
		TemperatureKey k2 = (TemperatureKey) w2;

		int result = k1.getYearMonth().compareTo(k2.getYearMonth());
		if (0 == result) {
			result = -1 * k1.getTemperature().compareTo(k2.getTemperature());
		}
		return result;
	}
}
