# Page Rank
—————————————————
STANDALONE MODE
—————————————————
Building and execution of PageRank in Standalone mode.

1. Create a new Maven Project in the IDE.

2. Give the Group ID and the artifact ID.

3. A pom.xml will be created, add the required dependencies. 

4. The src/main/java should contain the source code files.
	
5. Create an input folder which should contain the input file (provided in the assignment).
wikipedia-simple-html.bz2

6. Include the config folder which has the stand-alone configurations: core-site.xml, hdfs-site.xml, mapred-site.xml, yarn-site.xml. These can be obtained from where your hadoop is installed on your system. 

7. In the Makefile for standalone mode change
hadoop.root = hadoop location on your local file system
jar.name = jar file that will be created
jar.path = target/${jar.name}
job.name = change it to the path of your main file

8. pom.xml, Makefile, src, config and input folders should be in the same project directory.

After the above steps:
Run 2 commands into the terminal of the IDE
1. make switch-standalone.

2. make alone.

Once the make alone finishes execution we will get a target folder and an output folder. The target and output folders are deleted and created every time we run make alone.

The target folder contains the .class files and .jar file.

Following will be the output folders:
 For preprocessing (output0)
 For the 10 iterations of Page Rank (output1-output10)
 For the final delta calculation (outputFinalPageRank) 
 For the final top k output (output)

—————————————————
AWS EMR
—————————————————

1. Add the following plugin in pom.xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-jar-plugin</artifactId>
    <version>2.4</version>
    <configuration>
        <archive>
             <manifest>
                 <mainClass>{package_name.name_of_main_class}</mainClass>
             </manifest>
        </archive>
    </configuration>
</plugin>

This tells where to find the main class of the program you want to execute.

2. In the Makefile for AWS execution change
aws.region = to the region of your aws cluster
aws.bucket.name = bucket_name you want to create on S3
aws.subnet.id = subnet_id of your region from VPC subnet list
aws.input = name of the input folder on S3
aws.output = name of the output folder on S3
aws.log.dir = name of the log folder
aws.num.nodes =  number of worker machines
aws.instance.type = type of the machine to use.

For run-1 your aws.num.nodes=5 and aws.instance.type=m4.large
For run-2 your aws.num.nodes=10 and aws.instance.type=m4.large

3. On the terminal go to the directory where your source code, Makefile, pom.xml, input folder and .jar file exists. In the empty input folder add the 4 files mentioned in the assignment (full wiki data set for 2006).
Upload data on S3 with : make upload-input-aws

This will upload the data in your input folder and will make a bucket on S3 with the name as in your aws.bucket.name (in Makefile)

4. Log into the console on AWS.

5. Run ‘make cloud’ on the terminal to launch your cluster on EMR.

6. Go to the AWS console, to the cluster to see how the Cluster is running, it will take about 70-80 minutes for run1 and 40-50 minutes for run2. Once it’s terminated with steps completed, check the syslog.

7. Go to S3 and in the output folder you will see a part-r-0000? file which will have your final 100 pages and page ranks. Download that part-r-0000?.

The commands executed from step 3-7 should all be executed in that same directory on the terminal.

———————————————————————————————————————————————————
DIRECTORY STRUCTURE OF THE PROGRAMS
———————————————————————————————————————————————————
For the Intellij IDE

1. File Structure : PageRank/src/main/java

a. Main Files: 

1. PageRankMain.java: Class from where all jobs are called.

2. PageDetails.java: This class stores details about the page: page names, outlines and page rank.

3. PageRankPreProcess.java : The first pre-processing job is done here. In this map reduce job we parse the bz2 file , get page names with their out-links.

4. PageRankCalculate.java: The page ranks for 10 iterations are calculated.

5. PageRankFinalDelta.java: The page rank for the final iteration. Since delta updation of page ranks is performed in the map task in the previous file, we need an extra map task for the final correct page ran of the 10th iteration. Here I haven’t declared the reduce task, so by default the reduce tasks will be set to 1 and identity reducer will run.

6. PageRankTopK.java: Class which calculates the top 100 page ranks and their pages.

7. Bz2WikiParser.java: Parsing the compressed Bz2 file. Called from PageRankPreProcess.java

8. ParseXML.java : Class which parses the file into human readable format. This is not a part of the main code. It is just for the user to read the XML and determine the contents of the XML. 
Here I have given a default file to parse. You can change the path to the file you want to parse.


NOTE:
Here I am joining the data by ", ". On further observation, 
I saw that there were a few page names which had this delimiter. 
So I changed the delimiter to "~" since we are removing all the pages with "~".
I ran it with these changes on the local machine and didn't observe major changes
in the final output. As I already had run my code with ", " delimiter on AWS
by the time I discovered the delimiter discrepancy , I did not run it again on AWS EMR

For a normal run (not from the terminal) on the IDE,

Run->Edit Configurations->Applications
Click on the configuration tab on the right side pane
1. Enter your path for your Main class
2. Program arguments : input output (folders from input will be taken for the code and output generated respectively)

Then click on the run button for a successful run

If you get an error “log4j:WARN No appenders could be found for logger (org.apache.hadoop.metrics2.lib.MutableMetricsFactory)” ; include the log4j.properties file in src/main/resources/

Add the following into the log4j.properties file:

hadoop.root.logger=DEBUG, console
log4j.rootLogger = DEBUG, console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.out
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n

Following will be the output folders:
 For preprocessing (output0)
 For the 10 iterations of Page Rank (output1-output10)
 For the final delta calculation (outputFinalPageRank) 
 For the final top k output (output)
