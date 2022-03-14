#csvsorter
csvsorter sorts a csv input file using external sorting merge sort algorithm.

## Prerequisites
* java 8
* maven 3

## Getting Started
get the sources of this project to your local machine.
git repo -->

first, build the project, open terminal where pom.xml file is, and run:
```
mvn clean install
```
or use your IDE to build with maven.

to create javadoc site run
```
mvn javadoc:javadoc
```
the maven build will create an executable jar under the target folder
to run the program run 
```
java -jar csvsorter-1.0-SNAPSHOT.jar <args>
```
or use you IDE to run Main class.
you will need to pass 3 input parameters, see usage:
```
usage: csvsorter
-in,--input <arg>     input file path
-key,--keyind <arg>   sorting key index - the index of the field to sort
by
-max,--maxrec <arg>   maximum number of records in memory
-out,--output <arg>   output path (optional)
```
for your convenience, a sample csv file with ~30000 records is included in the project.
usage example:
````
java -jar csvsorter-1.0-SNAPSHOT.jar -in test-classes\employee_info.csv -key 2 -max 2000
````
if you don't set the output path, the default will be used - "target\sorted.csv"
another example, with output argument:
````
java -jar csvsorter-1.0-SNAPSHOT.jar -in test-classes\employee_info.csv -key 2 -max 2000 -out path/to/output/out.csv 
````

in case you want to run the project from your ide and still use the sample file, 
define the following argument in your IDE's run configuration (key and max are just a example):
```
-in=src\test\resources\employee_info.csv -key=2 -max=2000
```

## Built With
* [Maven](https://maven.apache.org/) - Dependency Management


## Author
* **Gal Palmery** 

## Algorithm description 
**map:**  
1. divide the input file to smaller files the size of maxRecordsNumber (also an input to the algorithm)
2. sort each part in memory using merge sort algorithm: 
   1. copy the input part (list) and divide to 2 lists - left and right - according to the middle index. 
   2. iterate over both parts and compare each entry according to the sorting key
   3. set the part list with the smaller entry (lexicographically) on the left index or the grater entry on the right index.
   4. advance the indices accordingly until all the list is sorted and return it.  
**reduce**
3. take each 2 sorted parts - output of the map part, that are now written to files  
4. read line by line, and compare the lines according to the sorting key
5. write the smaller entry to a new merged file
6. call recursively to the reduce function until there is only 1 file left, which will be the output

## complexity calculations

the map part is done in 2*O(n) + O(k*log(maxRecords)) time, where n is the number of records in the input csv file:
0(n) to count the lines, and another o(n) to go over the file line by line and write each part to a new file.
o(k*log(maxRecords)) is the time it takes to sort each of the parts in memory, where k is the number of parts, 
and maxRecords is the number of records in each file.

each reduce iteration is done in O(n*log(k)) where k is the number of parts that the input file was divided to.
and the recursion depth is log(n) / log(k). so entire reduce part sums up to O(n*log(n))

map part is insignificant in big O since O(n) is smaller than o(n*log(n)), so the total complexity calculation of the algorithm is O(n*log(n)).

parallelization does not affect the time complexity, since the number of threads is, as large as it might be, still a fixed number.
so it will also be insignificant in big O.

