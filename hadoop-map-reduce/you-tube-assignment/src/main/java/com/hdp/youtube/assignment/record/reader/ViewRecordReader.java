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

import com.hdp.youtube.assignment.record.ViewedRecord;

/**
 * @author Naveen
 *
 */
public class ViewRecordReader extends RecordReader<Text, ViewedRecord> {

	private LineRecordReader lineRecordReader;
	private ViewedRecord youtubeRecord;
	private Text key;

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
			youtubeRecord = null;
			return false;
		}

		// otherwise take a line and parse it

		Text line = lineRecordReader.getCurrentValue();
		String[] lineData = line.toString().split("\t");

		if (lineData == null || lineData.length <= 5)
			return true;

		key = new Text(lineData[0]);
		youtubeRecord = new ViewedRecord(new Text(lineData[5]), new Text("View"));

		System.out.println(youtubeRecord);
		return true;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public ViewedRecord getCurrentValue() throws IOException, InterruptedException {
		return youtubeRecord;
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
		youtubeRecord = null;
	}

}
