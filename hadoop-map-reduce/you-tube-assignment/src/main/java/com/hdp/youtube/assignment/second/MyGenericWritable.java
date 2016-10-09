package com.hdp.youtube.assignment.second;

import java.util.Arrays;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

import com.hdp.youtube.assignment.second.MultipleInputsTest.FirstClass;
import com.hdp.youtube.assignment.second.MultipleInputsTest.SecondClass;

@SuppressWarnings("unchecked")
public class MyGenericWritable extends GenericWritable {

	private static Class<? extends Writable>[] CLASSES = null;

	static {
		CLASSES = (Class<? extends Writable>[]) new Class[] { FirstClass.class, SecondClass.class };
	}

	// this empty initialize is required by Hadoop
	public MyGenericWritable() {
	}

	public MyGenericWritable(Writable instance) {
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
