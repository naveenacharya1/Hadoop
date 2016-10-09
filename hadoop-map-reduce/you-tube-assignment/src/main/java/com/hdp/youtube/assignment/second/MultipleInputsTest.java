package com.hdp.youtube.assignment.second;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MultipleInputsTest {

	public static class FirstMap extends Mapper<Text, FirstClass, Text, MyGenericWritable> {
	     public void map(Text key, FirstClass value, Context context) throws IOException, InterruptedException {
	         //System.out.println("FirstMap:" + key.toString() + " " + value.toString());
	         context.write(key, new MyGenericWritable(value));
	     }
	}

	public static class SecondMap extends Mapper<Text, SecondClass, Text, MyGenericWritable> {
	     public void map(Text key, SecondClass value, Context context) throws IOException, InterruptedException {
	        // System.out.println("FirstMap:" + key.toString() + " " + value.toString());
	         context.write(key, new MyGenericWritable(value));
	     }
	}


	
	public class Reduce extends Reducer<Text, MyGenericWritable, Text, Text> {
	    public void reduce(Text key, Iterable<MyGenericWritable> values, Context context) throws IOException, InterruptedException {
	        for (MyGenericWritable value : values) {
	            Writable rawValue = value.get();
	            if(rawValue instanceof FirstClass){
	                FirstClass firstClass = (FirstClass)rawValue;
	                System.out.println(firstClass);
	            	context.write(key, new Text (firstClass.toString()));
	            }
	            if(rawValue instanceof SecondClass){
	                SecondClass secondClass = (SecondClass)rawValue;
	                System.out.println(secondClass);
	                context.write(key, new Text (secondClass.toString()));
	            }
	        }
	    }
	}
	
	public static class FirstClass implements Writable {
		private String value;

		public FirstClass() {
			this.value = "TEST";
		}

		public FirstClass(String val) {
			this.value = val;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			if (null == in) {
				throw new IllegalArgumentException("in cannot be null");
			}
			String value = in.readUTF();

			this.value = value.trim();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			if (null == out) {
				throw new IllegalArgumentException("out cannot be null");
			}
			out.writeUTF(this.value);
		}

		@Override
		public String toString() {
			return "FirstClass\t" + value;
		}

	}

	public static class SecondClass implements Writable {

		private String value;
		private int additional;

		public SecondClass() {
			this.value = "TEST";
			this.additional = 0;
		}

		public SecondClass(String val, int addi) {
			this.value = val;
			this.additional = addi;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			if (null == in) {
				throw new IllegalArgumentException("in cannot be null");
			}
			String value = in.readUTF();
			int addi = in.readInt();

			this.value = value.trim();
			this.additional = addi;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			if (null == out) {
				throw new IllegalArgumentException("out cannot be null");
			}
			out.writeUTF(this.value);
			out.writeInt(this.additional);
		}
		@Override
		public String toString() {
			return "SecondClass\t" + value + "\t" + additional;
		}
	}

	public static class FirstClassReader extends RecordReader<Text, FirstClass> {
		private LineRecordReader lineRecordReader = null;
		private Text key = null;
		private FirstClass valueFirstClass = null;

		@Override
		public void close() throws IOException {
			if (null != lineRecordReader) {
				lineRecordReader.close();
				lineRecordReader = null;
			}
			key = null;
			valueFirstClass = null;
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public FirstClass getCurrentValue() throws IOException, InterruptedException {
			return valueFirstClass;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return lineRecordReader.getProgress();
		}

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
				valueFirstClass = null;
				return false;
			}

			// otherwise, take the line and parse it
			Text line = lineRecordReader.getCurrentValue();
			String str = line.toString();
			//System.out.println("FirstClass:" + str);
			String[] arr = str.split("\t", -1);

			key = new Text(arr[0].trim());
			valueFirstClass = new FirstClass(arr[1].trim());

			return true;
		}

	}

	public static class SecondClassReader extends RecordReader<Text, SecondClass> {

		private LineRecordReader lineRecordReader = null;
		private Text key = null;
		private SecondClass valueSecondClass = null;

		@Override
		public void close() throws IOException {
			if (null != lineRecordReader) {
				lineRecordReader.close();
				lineRecordReader = null;
			}
			key = null;
			valueSecondClass = null;
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public SecondClass getCurrentValue() throws IOException, InterruptedException {
			return valueSecondClass;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return lineRecordReader.getProgress();
		}

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
				valueSecondClass = null;
				return false;
			}

			// otherwise, take the line and parse it
			Text line = lineRecordReader.getCurrentValue();

			String str = line.toString();
			
			//System.out.println("SecondClass:" + str);
			String[] arr = str.split("\t", -1);
			int addi = Integer.parseInt(arr[2]);

			key = new Text(arr[0].trim());
			valueSecondClass = new SecondClass(arr[1].trim(), addi);

			return true;
		}

	}

	public static class FirstInputFormat extends FileInputFormat<Text, FirstClass> {
		@Override
		public RecordReader<Text, FirstClass> createRecordReader(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			return new FirstClassReader();
		}
	}

	public static class SecondInputFormat extends FileInputFormat<Text, SecondClass> {
		@Override
		public RecordReader<Text, SecondClass> createRecordReader(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			return new SecondClassReader();
		}
	}

	/**
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 * @param args
	 * @throws
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

	

		Path firstPath = new Path("hdfs://127.0.0.1:9000/input/FirstClassData.txt");
		Path secondPath = new Path("hdfs://127.0.0.1:9000/input/SecondClassData.txt");
		Path outputPath = new Path("hdfs://127.0.0.1:9000/output/assignment/youtube/topviewed/ex");
		
		Configuration conf = new Configuration();

		Job job = Job.getInstance();
		job.setJarByClass(MultipleInputsTest.class);
		job.setJobName("MultipleInputs Test");

		//job.setReducerClass(MyReducer.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(Text.class);
		job.setMapOutputValueClass(MyGenericWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.out.println("start");

		MultipleInputs.addInputPath(job, firstPath, FirstInputFormat.class, FirstMap.class);
		MultipleInputs.addInputPath(job, secondPath, SecondInputFormat.class, SecondMap.class);

		FileOutputFormat.setOutputPath(job, outputPath);
		
		FileSystem hdfs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), conf);
		if (hdfs.exists(outputPath))
			hdfs.delete(outputPath, true);

		job.waitForCompletion(true);

	}

}
