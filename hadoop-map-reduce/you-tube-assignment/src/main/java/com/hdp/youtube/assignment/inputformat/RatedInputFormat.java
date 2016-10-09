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
import com.hdp.youtube.assignment.record.RatedRecord;
import com.hdp.youtube.assignment.record.reader.RatedRecordReader;

/**
 * @author Naveen
 *
 */
public class RatedInputFormat extends FileInputFormat<Text, RatedRecord> {

	@Override
	public RecordReader<Text, RatedRecord> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new RatedRecordReader();
	}

}
