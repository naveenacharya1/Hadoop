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
public class RatedRecord implements Writable {

	private Text rating;
	private Text recordType;

	public RatedRecord(Text rating, Text recordType) {
		super();
		this.rating = rating;
		this.recordType = recordType;
	}

	public void write(DataOutput out) throws IOException {
		rating.write(out);
		recordType.write(out);

	}

	public void readFields(DataInput in) throws IOException {
		rating.readFields(in);
		recordType.readFields(in);
	}

	public Text getRating() {
		return rating;
	}

	public Text getRecordType() {
		return recordType;
	}


}
