# MapReduce - Secondary Sort Program

## Prerequisite
Apached Hadoop 2.7.1  
Apache Maven 3.3.9  
Java version: 1.8.0_101, vendor: Oracle Corporation  
Default locale: en_SG, platform encoding: Cp1252  
OS name: "windows 10", version: "10.0", arch: "amd64", family: "dos"  
Eclipse Java EE IDE for Web Developers. Version: Mars.2 Release (4.5.2)  

* Imagine we have temperature data that looks like the following. Each line represents the value of a temperature at a particular day. Each value in a line is delimited by a comma. The first value is the YEAR, the second value is the MONTH, and the third value is the TEMPERATURE value. The data below is a toy data set. As you can see, there are three Year: 2012, 2001, and 2005. The Month are also simple: 01, 11, 08. The values are fake as well: 5,45,35,10,46,47,48,50,52,38 and 70.

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
* Let’s say we want for each year and month (the reducer key input, or alternatively, the mapper key output), to order the values descendingly by temperature when they come into the reducer. How do we sort the temperature values descendingly? This problem is known as secondary sorting. Hadoop’s M/R platform sorts the keys, but not the values.

Expected output is:
```bash
201201:  5,10,35,45
200111: 40,46,47,48
200508: 38,50,52,70
```
A solution for secondary sorting involves doing multiple things. Since we know Year and Month can form a Key, we make combination of YEARMONTH as a KEY and 
First, instead of simply emitting the YEARMONTH as the key from the mapper, we need to emit a composite key, a key that has multiple parts. (Composite is a combination of key and a part of value or complete value). Now in our example, the key will have the YEARMONTH and TEMPERATURE. If you remember, the process for a M/R Job is as follows.

```bash
(K1,V1) –> Map –> (K2,V2)
(K2,List[V2]) –> Reduce –> (K3,V3)
```

In the toy data above, K1 will be of type LongWritable, and V1 will be of type Text. Without secondary sorting, K2 will be of type Text and V2 will be of type IntWritable (we simply emit the YEARMONTH and price from the TEMPERATURE to the reducer).  
So, K2=YEARMONTH, and V2=TEMPERATURE, or (K2,V2) = (YEARMONTH,TEMPERATURE). However, if we emit such an intermediary key-value pair, secondary sorting is not possible. We have to emit a composite key, K2={YEARMONTH,TEMPERATURE}. So the intermediary key-value pair is (K2,V2) = ({YEARMONTH,TEMPERATURE},TEMPERATURE). Note that composite data structures, such as the composite key, is held within the curly braces. Our reducer simply outputs a K3 of type IntWritable and V3 of type IntWritable; (K3,V3) = (YEARMONTH, TEMPERATURE). The complete M/R job with the new composite key is shown below.
```bash
(LongWritable,Text) –> Map –> ({YEARMONTH,TEMPERATURE},TEMPERATURE)
({YEARMONTH,TEMPERATURE},List[TEMPERATURE]) –> Reduce –> (YEARMONTH,TEMPERATURE)
```

K2 is a composite key, but inside it, the symbol part/component is referred to as the _natural_ key. It is the key which values will be grouped by.

#USE A COMPOSITE KEY COMPARATOR
The composite key comparator is where the secondary sorting takes place. It compares composite key by YEARMONTH ascendingly and TEMPERATURE descendingly. It is shown below. Notice here we sort based on YEARMONTH and TEMPERATURE. All the components of the composite key is considered.

```bash
public class CompositeKeyComparator extends WritableComparator {
    protected CompositeKeyComparator() {
        super(TemperatureKey.class, true);
    }   
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        TemperatureKey k1 = (TemperatureKey)w1;
        TemperatureKey k2 = (TemperatureKey)w2;
         
        int result = k1.getYearMonth().compareTo(k2.getYearMonth());
        if(0 == result) {
            result = -1* k1.getTemperature().compareTo(k2.getTemperature());
        }
        return result;
    }
}
```
##USE A NATURAL KEY GROUPING COMPARATOR

The natural key group comparator _groups_ values together according to the natural key. Without this component, each K2={YEARMONTH,TEMPERATURE} and its associated V2=TEMPERATURE may go to different reducers. Notice here, we only consider the _natural_ key.

```bash
public class NaturalKeyGroupingComparator extends WritableComparator {
    protected NaturalKeyGroupingComparator() {
        super(TemperatureKey.class, true);
    }   
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        TemperatureKey k1 = (TemperatureKey)w1;
        TemperatureKey k2 = (TemperatureKey)w2;
         
        return k1.getYearMonth().compareTo(k2.getYearMonth());
    }
}
```

##USE A NATURAL KEY PARTITIONER

The natural key partitioner uses the natural key to partition the data to the reducer(s). Again, note that here, we only consider the _natural_ key.


```bash
public class NaturalKeyPartitioner extends Partitioner<StockKey, DoubleWritable> {
 
    @Override
    public int getPartition(TemperatureKey key, IntWritable val, int numPartitions) {
        int hash = key.getYearMonth().hashCode();
        int partition = hash % numPartitions;
        return partition;
    }
 
}
```

##THE M/R JOB

Once we define the Mapper, Reducer, natural key grouping comparator, natural key partitioner, composite key comparator, and composite key, in Hadoop’s new M/R API, we may configure the Job as follows.

```bash

public class SsJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new SsJob(), args);
    }   
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, "secondary sort");
         
        job.setJarByClass(SsJob.class);
        job.setPartitionerClass(NaturalKeyPartitioner.class);
        job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);
         
        job.setMapOutputKeyClass(TemperatureKey.class);
        job.setMapOutputValueClass(IntWritable.class);
         
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
         
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
         
        job.setMapperClass(TemperatureMapper.class);
        job.setReducerClass(TemperatureReducer.class);
         
        job.waitForCompletion(true);
         
        return 0;
    }
}
```

Sample input/result files are provided in the project under resources/input, resources/output
