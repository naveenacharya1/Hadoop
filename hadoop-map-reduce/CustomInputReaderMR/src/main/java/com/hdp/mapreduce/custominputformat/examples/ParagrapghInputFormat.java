package com.hdp.mapreduce.custominputformat.examples;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

public class ParagrapghInputFormat extends TextInputFormat {
	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, JobConf conf, Reporter reporter)
			throws IOException {
		reporter.setStatus(split.toString());
		return new ParagraphRecordReader(conf, (FileSplit) split);
	}
}
