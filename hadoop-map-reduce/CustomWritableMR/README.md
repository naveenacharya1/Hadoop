# MapReduce - Custom Writable Program

In this example we need to count the frequency of the occurrence of two words together in the text. So we are going to define a custom class that is going to hold the two words together.  

This works with a local-standalone, pseudo-distributed or fully-distributed Hadoop installation (Single Node Setup).

## Prerequisite
Apached Hadoop 2.7.1  
Apache Maven 3.3.9  
Java version: 1.8.0_101, vendor: Oracle Corporation  
Default locale: en_SG, platform encoding: Cp1252  
OS name: "windows 10", version: "10.0", arch: "amd64", family: "dos"  
Eclipse Java EE IDE for Web Developers. Version: Mars.2 Release (4.5.2)  

## What is a Writable in Hadoop?

Writable in an interface in Hadoop and types in Hadoop must implement this interface. Hadoop provides these writable wrappers for almost all Java primitive types and some other types.  

Now the obvious question is why does Hadoop use these types instead of Java types?  
##Why does Hadoop use Writable(s)?  
As we already know, data needs to be transmitted between different nodes in a distributed computing environment. This requires serialization and deserialization of data to convert the data that is in structured format to byte stream and vice-versa. Hadoop therefore uses simple and efficient serialization protocol to serialize data between map and reduce phase and these are called Writable(s).    

##Custom Writable
So any user defined class that implements the Writable interface is a custom writable. However, we need a custom Writable comparable if our custom data type is going to be used as key rather that the value. We then need the class to implement WritableComparable interface. 
 
##Input Data
```bash
What do you mean by Hadoop
What do you know about spark
What is Hadoop File System
How Hadoop executes mapreduce program
```
##Output
```bash
File System	1
Hadoop File	1
Hadoop What	1
Hadoop executes	1
How Hadoop	1
System How	1
What do	2
What is	1
about spark	1
by Hadoop	1
do you	2
executes mapreduce	1
is Hadoop	1
know about	1
mapreduce program	1
mean by	1
spark What	1
you know	1
you mean	1
```

Sample input/result files are provided in the project under resources/input, resources/output
