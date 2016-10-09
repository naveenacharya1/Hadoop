/**
 * 
 */
package com.hdp.youtube.assignment.record.reader;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import com.hdp.youtube.assignment.record.RatedRecord;

/**
 * @author Naveen
 *
 */
public class RatedRecordReader extends RecordReader<Text, RatedRecord> {

	private Text key;
	private RatedRecord ratedRecord;
	private LineRecordReader lineRecordReader;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		close();
		lineRecordReader = new LineRecordReader();
		lineRecordReader.initialize(split, context);

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!lineRecordReader.nextKeyValue()) {
			key = null;
			ratedRecord = null;
			return false;

		}

		Text currentLine = lineRecordReader.getCurrentValue();
		String[] lineData = currentLine.toString().split("\t");

		if (lineData == null || lineData.length <= 6)
			return true;

		key = new Text(lineData[0]);
		ratedRecord = new RatedRecord(new Text(lineData[6]), new Text("rating"));
		System.out.println(ratedRecord);
		return true;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public RatedRecord getCurrentValue() throws IOException, InterruptedException {
		return ratedRecord;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return lineRecordReader.getProgress();
	}

	@Override
	public void close() throws IOException {
		if (lineRecordReader != null) {
			lineRecordReader.close();
			lineRecordReader = null;
		}

		key = null;
		ratedRecord = null;
	}

}
