/**
 * 
 */
package com.hdp.youtube.assignment.inputformat;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.hdp.youtube.assignment.record.ViewedRecord;
import com.hdp.youtube.assignment.record.reader.ViewRecordReader;

/**
 * @author Naveen
 *
 */
public class ViewInputFormat extends FileInputFormat<Text, ViewedRecord> {

	@Override
	public RecordReader<Text, ViewedRecord> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new ViewRecordReader();
	}

}
