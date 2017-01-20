/**
 * 
 */
package com.hdp.youtube.assignment.record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * @author Naveen
 *
 */
public class ViewedRecord implements Writable {
	private Text viewCount;
	private Text recordType;

	public ViewedRecord(Text viewCount, Text recordType) {
		super();
		this.viewCount = viewCount;
		this.recordType = recordType;
	}

	public void write(DataOutput out) throws IOException {
		viewCount.write(out);
		recordType.write(out);

	}

	public void readFields(DataInput in) throws IOException {
		viewCount.readFields(in);
		recordType.readFields(in);
	}

	public Text getViewCount() {
		return viewCount;
	}

	public Text getRecordType() {
		return recordType;
	}

}
