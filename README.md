## Team members' names 

1. Student Name: Prathamesh Shenoy

   Student UT EID: ps33623


##  Course Name: CS378 - Cloud Computing 

##  Unique Number: 51515
    
## 5.1 Task 1 Results
```
1	121595
2	94135
3	72210
4	56264
5	44636
6	34874
7	60901
8	99447
9	122434
10	130585
11	127609
12	129444
13	139121
14	138864
15	145772
16	147908
17	132116
18	139500
19	173577
20	182650
21	171167
22	167405
23	162694
24	146839
```



## 5.2 Task 2 Results
```
00DC83118CA675B9A2876C35E3398AF5	1.0
0219EB9A4C74AAA118104359E5A5914C	1.0
022B8DF4D6D7C4DCF11233DD74C9E189	1.0
02510B3B0E797E51AF73361185F62D0B	1.0
FF96A951C04FBCEDE5BCB473CF5CBDBF	1.0
```

## 5.3 Task 3 Results
```
2CB4FE05D307D6294A6E31C00E5F2755	48.995
E8883F90EA31DFD66AE6E347BFD64F66	18.472908
E766E11C249A6D9A8D68C92D1B8BE094	18.4375
E2C420D928D4BF8CE0FF2EC19B371514	17.5
E77A964307CF49B32AD77E298A4951D0	16.769545
AD61AB143223EFBC24C7D2583BE69251	15.794286
C81E728D9D4C2F636F067F89CC14862C	12.558824
98F13708210194C475687BE6106A3B84	7.2032967
C9B9AE1B4267024D489995D8371BBB3B	5.555
45C48CCE2E2D7FBDEA1AFC51C7C6AD26	5.080645
```




# Project Template

# Running on Laptop     ####

Prerequisite:

- Maven 3

- JDK 1.6 or higher

- (If working with eclipse) Eclipse with m2eclipse plugin installed


The java main class is:

edu.cs.utexas.HadoopEx.WordCount 

Input file:  Book-Tiny.txt  

Specify your own Output directory like 

# Running:




## Create a JAR Using Maven 

To compile the project and create a single jar file with all dependencies: 
	
```	mvn clean package ```



## Run your application
Inside your shell with Hadoop

Running as Java Application:

```java -jar target/MapReduce-WordCount-example-0.1-SNAPSHOT-jar-with-dependencies.jar SOME-Text-Fiel.txt  output``` 

Or has hadoop application

```hadoop jar your-hadoop-application.jar edu.cs.utexas.HadoopEx.WordCount arg0 arg1 ... ```



## Create a single JAR File from eclipse



Create a single gar file with eclipse 

*  File export -> export  -> export as binary ->  "Extract generated libraries into generated JAR"
