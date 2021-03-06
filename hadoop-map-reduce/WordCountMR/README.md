# MapReduce - Word Count Program

WordCount is a simple application that counts the number of occurrences of each word in a given input set.
This works with a local-standalone, pseudo-distributed or fully-distributed Hadoop installation (Single Node Setup).

## Prerequisite
Apached Hadoop 2.7.1  
Apache Maven 3.3.9  
Java version: 1.8.0_101, vendor: Oracle Corporation  
Default locale: en_SG, platform encoding: Cp1252  
OS name: "windows 10", version: "10.0", arch: "amd64", family: "dos"  
Eclipse Java EE IDE for Web Developers. Version: Mars.2 Release (4.5.2)  

## Inputs and Outputs

The MapReduce framework operates exclusively on <key, value> pairs, that is, the framework views the input to the job as a set of <key, value> pairs and produces a set of <key, value> pairs as the output of the job, conceivably of different types.  

The key and value classes have to be serializable by the framework and hence need to implement the Writable interface. Additionally, the key classes have to implement the WritableComparable interface to facilitate sorting by the framework.  

Input and Output types of a MapReduce job:  

(input) <k1, v1> -> map -> <k2, v2> -> combine -> <k2, v2> -> reduce -> <k3, v3> (output)  

Sample input/result files are provided in the project under resources/input, resources/output
