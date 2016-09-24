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
```bash
ID		Name	Age		Gender	Salary
```
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

##Output

Output in Part-00000
```bash
Female	$13000
Male	$3000
```
Output in Part-00001
```bash
Female	$12000
Male	$6000
```
Output in Part-00002
```bash
Female	$7000
Male	$15000
```
Sample input/result files are provided in the project under resources/input, resources/output
