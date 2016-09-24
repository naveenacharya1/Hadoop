# MapReduce - Partitioner Program  

* A partitioner works like a condition in processing an input dataset. The partition phase takes place after the Map phase and before the Reduce phase.  

* The number of partitioners is equal to the number of reducers. That means a partitioner will divide the data according to the number of reducers. Therefore, the data passed from a single partitioner is processed by a single Reducer.  

## Prerequisite
Apached Hadoop 2.7.1  
Apache Maven 3.3.9  
Java version: 1.8.0_101, vendor: Oracle Corporation  
Default locale: en_SG, platform encoding: Cp1252  
OS name: "windows 10", version: "10.0", arch: "amd64", family: "dos"  
Eclipse Java EE IDE for Web Developers. Version: Mars.2 Release (4.5.2)  

## Input/Output

##Input Data

*ID*	*Name*	*Age* *Gender*	*Salary*  
```bash
1401	Mark	30		Male	$5000  
1402	Adam	34		Female	$7000  
1403	Philip	40		Male	$15000  
1404	Sam		44		Male	$8000  
1405	Joseph	34		Male	$9000  
1406	Andrew	22		Female	$12000  
1407	Julian	20		Female	$11000  
1408	Tim		18		Female	$13000  
1409	Tam		27		Male	$6000  
1410	Bob		18		Male	$3000  
```
We have to write an application to process the input dataset to find the highest salaried employee by gender in different age groups (for example, below 20, between 21 to 30, above 30).

Based on the given input, following is the algorithmic explanation of the program.

#Map Tasks  
The map task accepts the key-value pairs as input while we have the text data in a text file. The input for this map task is as follows −

##Input −  
The key would be a pattern such as “any special key + filename + line number” (example: key = @input1) and the value would be the data in that line (example: value = 1401 \t Mark \t 30 \t Male \t $5000).

Method − The operation of this map task is as follows −  

Read the value (record data), which comes as input value from the argument list in a string.  

Using the split function, separate the gender and store in a string variable.  




Sample input/result files are provided in the project under resources/input, resources/output
