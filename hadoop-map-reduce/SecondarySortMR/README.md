# MapReduce - Secondary Sort Program

Imagine we have temperature data that looks like the following. Each line represents the value of a temperature at a particular year and month. Each value in a line is delimited by a comma. The first value is the YEAR, the second value is the MONTH, and the third value is the TEMPERATURE value. The data below is a toy data set. As you can see, there are three Years: 2012, 2001, and 2005. The Month are also simple: 01, 11, 08. The values are fake as well: 5,45,35,10,46,47,48,50,52,38 and 70.

```bash
2012,01,5
2012,01,45
2012,01,35
2012,01,10
2001,11,46
2001,11,47
2001,11,48
2001,11,40
2005,08,50
2005,08,52
2005,08,38
2005,08,70
```
Let’s say we want for each year and month (the reducer key input, or alternatively, the mapper key output), to order the values descendingly by temperature when they come into the reducer. How do we sort the temperature values descendingly? This problem is known as secondary sorting. Hadoop’s M/R platform sorts the keys, but not the values.

Expected value in the reducer is :
```bash
200111 [48,47,46,40]
200508 [70,52,50,38]
201201 [45,35,10,5]
```
(YEARMONTH are sorted by ascending order and list of temperature values are sorted by descending order)  

A solution for secondary sorting involves doing multiple things. Since we know Year and Month can form a Key, we make combination of YEARMONTH as a KEY and instead of simply emitting the YEARMONTH as the key from the mapper, we need to emit a composite key, a key that has multiple parts. (Composite is a combination of key and a part of value or complete value). Now in our example, the key will be {YEARMONTH and TEMPERATURE} and value will be TEMPERATURE. If you remember, the process for a M/R Job is as follows.

```bash
(K1,V1) –> Map –> (K2,V2)
(K2,List[V2]) –> Reduce –> (K3,V3)
```

In the toy data above, K1 will be of type LongWritable, and V1 will be of type Text. Without secondary sorting, K2 will be of type Text and V2 will be of type IntWritable (we simply emit the YEARMONTH and TEMPERATURE from the mapper to the reducer).  
So, K2=YEARMONTH, and V2=TEMPERATURE, or (K2,V2) = (YEARMONTH,TEMPERATURE). However, if we emit such an intermediary key-value pair, secondary sorting is not possible. We have to emit a composite key, K2={YEARMONTH,TEMPERATURE}. So the intermediary key-value pair is (K2,V2) = ({YEARMONTH,TEMPERATURE},TEMPERATURE). Note that composite data structures, such as the composite key, is held within the curly braces. Our reducer simply outputs a K3 of type IntWritable and V3 of type IntWritable; (K3,V3) = (YEARMONTH, TEMPERATURE). The complete M/R job with the new composite key is shown below.
```bash
(LongWritable,Text) –> Map –> ({YEARMONTH,TEMPERATURE},TEMPERATURE)
({YEARMONTH,TEMPERATURE},List[TEMPERATURE]) –> Reduce –> (YEARMONTH,TEMPERATURE)
```

K2 is a composite key, but inside it, the YEARMONTH part/component is referred to as the _natural_ key. It is the key which values will be grouped by.

Lets try to write a program without using secondary sort and check the reducer output.  

TemperatureKey.java

```bash
package com.hdp.mapreduce.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class TemperatureKey implements WritableComparable<TemperatureKey> {

	private IntWritable yearMonth;
	private IntWritable temperature;

	public TemperatureKey() {
		set(new IntWritable(), new IntWritable());
	}

	public void set(IntWritable yearMonth, IntWritable temperature) {
		this.yearMonth = yearMonth;
		this.temperature = temperature;

	}

	public TemperatureKey(IntWritable yearMonth, IntWritable temperature) {
		set(yearMonth, temperature);
	}

	public void write(DataOutput out) throws IOException {
		yearMonth.write(out);
		temperature.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		yearMonth.readFields(in);
		temperature.readFields(in);
	}

	public int compareTo(TemperatureKey o) {
		return yearMonth.compareTo(o.yearMonth);
	}

	public IntWritable getYearMonth() {
		return yearMonth;
	}

	public IntWritable getTemperature() {
		return temperature;
	}

}

```
TemperatureMapper.java

```bash
package com.hdp.mapreduce.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TemperatureMapper extends MapReduceBase
		implements Mapper<LongWritable, Text, TemperatureKey, IntWritable> {

	public void map(LongWritable key, Text value, OutputCollector<TemperatureKey, IntWritable> output,
			Reporter reporter) throws IOException {

		try {
			String[] tempData = value.toString().split(",");
			TemperatureKey temperatureKey = new TemperatureKey();
			String keyData = tempData[0] + tempData[1];
			temperatureKey.set(new IntWritable(Integer.valueOf(keyData)),
					new IntWritable(Integer.valueOf(tempData[2])));
			output.collect(temperatureKey, new IntWritable(Integer.valueOf(tempData[2])));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}

```
TemperatureReducer.java

```bash
package com.hdp.mapreduce.secondarysort;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TemperatureReducer extends MapReduceBase
		implements Reducer<TemperatureKey, IntWritable, IntWritable, Text> {
	StringBuffer stringBuffer = new StringBuffer();

	public void reduce(TemperatureKey key, Iterator<IntWritable> values, OutputCollector<IntWritable, Text> output,
			Reporter reporter) throws IOException {
		try {
			stringBuffer.append("[");
			while (values.hasNext()) {
				stringBuffer.append(values.next()).append(",");
			}
			stringBuffer.setLength(stringBuffer.length() - 1);
			stringBuffer.append("]");
			output.collect(key.getYearMonth(), new Text(stringBuffer.toString()));
			System.out.println("Reducer Value Output :" + stringBuffer);
			stringBuffer.setLength(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

```

MapperJob.java

```bash

package com.hdp.mapreduce.secondarysort;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;

public class MapperJob {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Path inputPath = new Path("hdfs://127.0.0.1:9000/input/temperatue.txt");
		Path outputPath = new Path("hdfs://127.0.0.1:9000/output/temperatue/result");

		JobConf job = new JobConf(conf, MapperJob.class);
		job.setJarByClass(MapperJob.class);
		job.setJobName("SecSortJob");

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setOutputKeyClass(TemperatureKey.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setMapperClass(TemperatureMapper.class);
		job.setReducerClass(TemperatureReducer.class);

		FileSystem hdfs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), conf);
		if (hdfs.exists(outputPath))
			hdfs.delete(outputPath, true);

		RunningJob runningJob = JobClient.runJob(job);
		System.out.println("Job Successfull: " + runningJob.isComplete());
	}

}


```

part-r-00000
```bash
200111	[40,48,47,46]
200508	[70,38,52,50]
201201	[10,35,45,5]
```

Lets modify program to perform secondary sort

##Composite Key Comparator
The composite key comparator is where the secondary sorting takes place. It compares composite key by YEARMONTH ascendingly and TEMPERATURE descendingly. It is shown below. Notice here we sort based on YEARMONTH and TEMPERATURE. All the components of the composite key is considered.

```bash
/**
 * 
 */
package com.hdp.mapreduce.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author Naveen
 *
 */
public class CompositeKeyComparator extends WritableComparator {

	protected CompositeKeyComparator() {
		super(TemperatureKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		TemperatureKey k1 = (TemperatureKey) w1;
		TemperatureKey k2 = (TemperatureKey) w2;

		int result = k1.getYearMonth().compareTo(k2.getYearMonth());
		if (0 == result) {
			result = -1 * k1.getTemperature().compareTo(k2.getTemperature());
		}
		return result;
	}
}

```
##Grouping comparator

The natural key group comparator _groups_ values together according to the natural key. Without this component, each K2={YEARMONTH,TEMPERATURE} and its associated V2=TEMPERATURE may go to different reducers. Notice here, we only consider the _natural_ key.

```bash
/**
 * 
 */
package com.hdp.mapreduce.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/**
 * @author Naveen
 *
 */
public class NaturalKeyGroupingComparator extends WritableComparator {
	protected NaturalKeyGroupingComparator() {
		super(TemperatureKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		TemperatureKey k1 = (TemperatureKey) w1;
		TemperatureKey k2 = (TemperatureKey) w2;
		return k1.getYearMonth().compareTo(k2.getYearMonth());
	}
}

```

##Custom partitioner

In a nutshell, the partitioner decides which mapper’s output goes to which reducer based on the mapper’s output key. For this, we need two plug-in classes: a custom partitioner to control which reducer processes which keys, and a custom Comparator to sort reducer values. The custom partitioner ensures that all data with the same key (the natural key, not including the composite key with the temperature value) is sent to the same reducer. The custom Comparator does sorting so that the natural key (YEARMONTH) groups the data once it arrives at the reducer.

```bash
/**
 * 
 */
package com.hdp.mapreduce.secondarysort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Partitioner;

/**
 * @author Naveen
 *
 */
public class NaturalKeyPartitioner extends MapReduceBase implements Partitioner<TemperatureKey, IntWritable> {

	public int getPartition(TemperatureKey key, IntWritable value, int numPartitions) {

		int hash = key.getYearMonth().hashCode();
		int partition = hash % numPartitions;
		return partition;
	}
}

```

##THE M/R JOB

Once we define the Mapper, Reducer, natural key grouping comparator, natural key partitioner, composite key comparator, and composite key, in Hadoop’s new M/R API, we may configure the Job as follows.

```bash

package com.hdp.mapreduce.secondarysort;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;

public class MapperJob {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Path inputPath = new Path("hdfs://127.0.0.1:9000/input/temperatue.txt");
		Path outputPath = new Path("hdfs://127.0.0.1:9000/output/temperatue/result");

		JobConf job = new JobConf(conf, MapperJob.class);
		job.setJarByClass(MapperJob.class);
		job.setJobName("SecSortJob");

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setPartitionerClass(NaturalKeyPartitioner.class);
		job.setOutputValueGroupingComparator(NaturalKeyGroupingComparator.class);
		job.setOutputKeyComparatorClass(CompositeKeyComparator.class);

		job.setOutputKeyClass(TemperatureKey.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setMapperClass(TemperatureMapper.class);
		job.setReducerClass(TemperatureReducer.class);

		FileSystem hdfs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), conf);
		if (hdfs.exists(outputPath))
			hdfs.delete(outputPath, true);

		RunningJob runningJob = JobClient.runJob(job);
		System.out.println("Job Successfull: " + runningJob.isComplete());
	}

}

```

part-r-00000
```bash
200111	[48,47,46,40]
200508	[70,52,50,38]
201201	[45,35,10,5]
```

##Data Flow Using Plug-in Classes

To help you understand the map() and reduce() functions and custom plug-in classes,below diagram illustrates the data flow for a portion of input.  

```bash

----------------						   ----------------------
|  2012,01,5	|                          |  ((201201,5),5)	|
|  2012,01,45   |                          |  ((201201,45),45)  |
|  2012,01,35   |                          |  ((201201,35),35)  |
|  2012,01,10   | 		-------------      |  ((201201,10),10)  |	   ------------------
|  2001,11,46   | 		|			|      |  ((200111,46),46)  |      |				|
|  2001,11,47   | ----->|	map()	|----> |  ((200111,47),47)  |----> |	partition()	|
|  2001,11,48   | 		|			|      |  ((200111,48),48)  |      |				|
|  2001,11,40   | 		-------------      |  ((200111,40),40)  |      ------------------
|  2005,08,50   |                          |  ((200508,50),50)  |				|
|  2005,08,52   |                          |  ((200508,52),52)  |				|
|  2005,08,38   |                          |  ((200508,38),38)  |				|
|  2005,08,70   |                          |  ((200508,70),70)  |				|
----------------                           ----------------------				|
																				|
																				|
																				|
																				|
                                												|				
                                								---------------------------|	   
                                								|	---------------------  |	   
                                								|	|  ((201201,5),5)	|  |	
                                								|	|  ((201201,45),45) |  |	
                                								|	|  ((201201,35),35) |  |	
                                  ------------------- 			|	|  ((201201,10),10)	|  |	
 ---------------------------	  |					| 			|	|-------------------|  |					
 |  (200111 [40,48,47,46]) |	  |   group()		|<---------	|	|  ((200111,46),46) |  |				
 |  (200508	[70,38,52,50]) |<-----|   comparator()	|			|  	|  ((200111,47),47) |  |					
 |  (201201	[10,35,45,5])  |	  |	  reduce()  	|			|	|  ((200111,48),48) |  |								
 ---------------------------	  |					|  			|	|  ((200111,40),40)	|  |							
						          |-----------------|			|	|-------------------|  |		
                                								|	|  ((200508,50),50) |  |		
                                								|	|  ((200508,52),52) |  |		
						        								|	|  ((200508,38),38) |  |		
						        								|	|  ((200508,70),70) |  |		
					            								|	---------------------  |		
					            								----------------------------																		
                                                        
                                                        
```





Sample input/result files are provided in the project under resources/input, resources/output
