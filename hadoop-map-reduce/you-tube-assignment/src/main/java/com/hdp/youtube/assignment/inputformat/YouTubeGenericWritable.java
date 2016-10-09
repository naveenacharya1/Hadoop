/**
 * 
 */
package com.hdp.youtube.assignment.inputformat;

import java.util.Arrays;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

import com.hdp.youtube.assignment.record.RatedRecord;
import com.hdp.youtube.assignment.record.ViewedRecord;

/**
 * @author Naveen
 *
 */
@SuppressWarnings("unchecked")
public class YouTubeGenericWritable extends GenericWritable {

	private static Class<? extends Writable>[] CLASSES = null;

	static {
		CLASSES = (Class<? extends Writable>[]) new Class[] { ViewedRecord.class, RatedRecord.class };
	}

	// this empty initialize is required by Hadoop
	public YouTubeGenericWritable() {
	}

	public YouTubeGenericWritable(Writable instance) {
		set(instance);
	}

	@Override
	protected Class<? extends Writable>[] getTypes() {
		return CLASSES;
	}

	@Override
	public String toString() {
		return "MyGenericWritable [getTypes()=" + Arrays.toString(getTypes()) + "]";
	}

}
