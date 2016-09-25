/**
 * 
 */
package com.hdp.mapreduce.custominputformat.examples;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

/**
 * @author Naveen
 *
 */
public class ParagraphRecordReader implements RecordReader<LongWritable, Text> {

	private LineRecordReader lineRecord;
	private LongWritable lineKey;
	private Text lineValue;

	public ParagraphRecordReader(JobConf conf, FileSplit split) throws IOException {
		lineRecord = new LineRecordReader(conf, split);
		lineKey = lineRecord.createKey();
		lineValue = lineRecord.createValue();
	}

	public void close() throws IOException {
		lineRecord.close();

	}

	public LongWritable createKey() {
		return new LongWritable();
	}

	public Text createValue() {
		return new Text("");
	}

	public long getPos() throws IOException {
		return lineRecord.getPos();
	}

	public float getProgress() throws IOException {
		return lineRecord.getPos();
	}

	public boolean next(LongWritable key, Text value) throws IOException {
		boolean appended, isNextLineAvailable;
		boolean retval;
		byte space[] = { ' ' };
		value.clear();
		isNextLineAvailable = false;
		do {
			appended = false;
			retval = lineRecord.next(lineKey, lineValue);
			if (retval) {
				if (lineValue.toString().length() > 0) {
					byte[] rawline = lineValue.getBytes();
					int rawlinelen = lineValue.getLength();
					value.append(rawline, 0, rawlinelen);
					value.append(space, 0, 1);
					appended = true;
				}
				isNextLineAvailable = true;
			}
		} while (appended);

		return isNextLineAvailable;
	}

}
