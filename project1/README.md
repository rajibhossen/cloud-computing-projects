Description
The purpose of this project is to develop a simple Map-Reduce program on Hadoop that evaluates one step of k-means clustering.

This project must be done individually. No copying is permitted. Note: We will use a system for detecting software plagiarism, called Moss, which is an automatic system for determining the similarity of programs. That is, your program will be compared with the programs of the other students in class as well as with the programs submitted in previous years. This program will find similarities even if you rename variables, move code, change code structure, etc.

Note that, if you use a Search Engine to find similar programs on the web, we will find these programs too. So don't do it because you will get caught and you will get an F in the course (this is cheating). Don't look for code to use for your project on the web or from other students (current or past). Just do your project alone using the help given in this project description and from your instructor and GTA only.

Platform
You will develop your program on your laptop and then on SDSC Comet. Optionally, you may use IntelliJ IDEA or Eclipse to help you develop your program on your laptop, but you should test your programs on Comet before you submit them.

How to develop your project on your laptop
You can use your laptop to develop your program and then test it and run it on Comet. This is optional but highly recommended because it will save you a lot of time. Testing and running your program on Comet is required.

If you have Mac OSX or Linux, make sure you have Java and Maven installed (on Mac, you can install Maven using Homebrew brew install maven, on Ubuntu Linux, use apt install maven). If you have Windows 10, you may install Ubuntu Shell and do: sudo apt update, sudo apt upgrade, and sudo apt install openjdk-8-jdk maven.

To install Hadoop and project, cut&paste and execute on the shell:

cd
wget https://archive.apache.org/dist/hadoop/common/hadoop-2.6.5/hadoop-2.6.5.tar.gz
tar xfz hadoop-2.6.5.tar.gz
wget http://lambda.uta.edu/cse6331/project1.tgz
tar xfz project1.tgz
Update: I have updated the pom.xml files. Please download project1.tgz again (or update the pom.xml files by inserting two lines before hadoop.version):
  <properties>
    <maven.compiler.source>6</maven.compiler.source>
    <maven.compiler.target>1.6</maven.compiler.target>
    <hadoop.version>2.6.5</hadoop.version>
  </properties>
You should also set your JAVA_HOME to point to your java installation. For example, on Windows do:
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
To test Map-Reduce, go to project1/examples and look at the two Map-Reduce examples src/main/java/Simple.java and src/main/java/Join.java. You can compile both Java files using: mvn install and you can run Simple in standalone mode using:
~/hadoop-2.6.5/bin/hadoop jar target/*.jar Simple simple.txt output-simple
The file output-simple/part-r-00000 will contain the results.
To compile and run project1:

cd project1
mvn install
rm -rf output
~/hadoop-2.6.5/bin/hadoop jar target/*.jar KMeans points-small.txt centroids.txt output
After your project works correctly on your laptop, copy it to Comet:
cd
rm project1.tgz
tar cfz project1.tgz project1
scp project1.tgz xyz1234@comet.sdsc.edu:
where xyz1234 is your Comet username.
Setting up your Project on Comet
This step is required. If you'd like, you can develop this project completely on Comet. Follow the directions on How to login to Comet at comet.html. Please email the GTA if you need further help.

Edit the file .bashrc (note: it starts with a dot) using a text editor, such as nano .bashrc, and add the following 2 lines at the end (cut-and-paste):

alias run='srun --pty -A uot143 --partition=shared --nodes=1 --ntasks-per-node=1 --mem=5G -t 00:05:00 --wait=0 --export=ALL'
export project=/oasis/projects/nsf/uot143/fegaras
logout and login again to apply the changes. If you have already developed project1 on your laptop, copy project1.tgz from your laptop to Comet. Otherwise, download project1 from the class web site:
wget http://lambda.uta.edu/cse6331/project1.tgz
Untar it:
tar xfz project1.tgz
rm project1.tgz
chmod -R g-wrx,o-wrx project1
Go to project1/examples and look at the two Map-Reduce examples src/main/java/Simple.java and src/main/java/Join.java. You can compile both Java files using:
run example.build
and you can run them in standalone mode using:
sbatch example.local.run
The file example.local.out will contain the trace log of the Map-Reduce evaluation while the files output-simple/part-r-00000 output-join/part-r-00000 will contain the results.
You can compile KMeans.java on Comet using:

run kmeans.build
and you can run KMeans.java in standalone mode over a small dataset using:
sbatch kmeans.local.run
Please note that running in distributed mode will waste at least 10 of your SUs.
Project Description: K-means Clustering
In this project, you are asked implement one step of the Lloyd's algorithm for k-means clustering. The goal is to partition a set of points into k clusters of neighboring points. It starts with an initial set of k centroids. Then, it repeatedly partitions the input according to which of these centroids is closest and then finds a new centroid for each partition. That is, if you have a set of points P and a set of k centroids C, the algorithm repeatedly applies the following steps:

Assignment step: partition the set P into k clusters of points Pi, one for each centroid Ci, such that a point p belongs to Pi if it is closest to the centroid Ci among all centroids.
Update step: Calculate the new centroid Ci from the cluster Pi so that the x,y coordinates of Ci is the mean x,y of all points in Pi.
The datasets used are random points on a plane in the squares (i*2+1,j*2+1)-(i*2+2,j*2+2), with 0≤i≤9 and 0≤j≤9 (so k=100 in k-means). The initial centroids in centroid.txt are the points (i*2+1.2,j*2+1.2). So the new centroids should be in the middle of the squares at (i*2+1.5,j*2+1.5).
In this project, you are asked to implement one step of the K-means clustering algorithm. You should write one Map-Reduce job in the Java file src/main/java/KMeans.java. An empty src/main/java/KMeans.java has been provided, as well as scripts to build and run this code on Comet. You should modify the KMeans.java only.

To help you, I am giving you the pseudo code:

class Point {
    public double x;
    public double y;
}

Vector[Point] centroids;

mapper setup:
  read centroids from the distributed cache

map ( key, line ):
  Point p = new Point()
  read 2 double numbers from the line (x and y) and store them in p
  find the closest centroid c to p
  emit(c,p)

reduce ( c, points ):
  count = 0
  sx = sy = 0.0
  for p in points
      count++
      sx += p.x
      sy += p.y
  c.x = sx/count
  c.y = sy/count
  emit(c,null)
In your Java main program args[0] is the data point file (points-small.txt or points-large.txt), args[1] is the centroids.txt, args[2] is the output directory. Use job.addCacheFile(new URI(args[1])) to broadcast the file centroids.txt to all mappers, and Mapper.Context.getCacheFiles to access the broadcast file at the mapper setup (method setup):
URI[] paths = context.getCacheFiles();
Configuration conf = context.getConfiguration();
FileSystem fs = FileSystem.get(conf);
BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(paths[0]))));
then use reader.readLine() to read the lines from the file and store the centroids to the vector centroids.
You need to make the Point class WritableComparable. How do you compare 2 Points? Compare the x components first; if equal, compare the y components. You need to add a toString method for Point to print points. How do you find the closest centroid to a point p? Go through all centroids and find one whose Euclidean distance from p is minimum.
Optional: Use an IDE to develop your project
If you have a prior good experience with an IDE (IntelliJ IDEA or Eclipse), you may want to develop your program using an IDE and then test it and run it on Comet. Using an IDE is optional; you shouldn't do this if you haven't used an IDE before.

On IntelliJ IDEA, go to New→Project from Existing Sources, then choose your project1 directory, select Maven, and then Finish. To compile the project, go to Run→Edit Configurations, use + to Add New Configuration, select Maven, give it a name, use Working directory: your project1 directory, Command line: install, then Apply.

On Eclipse, you first need to install m2e (Maven on Eclipse), if it's not already installed. Then go to Open File...→Import Project from File System, then choose your project1 directory. To compile your project, right click on the project name at the Package Explorer, select Run As, and then Maven install.

Documentation
The The Map-Reduce API. The API has two variations for most classes: org.apache.hadoop.mapreduce and org.apache.hadoop.mapred. You should only use the classes in the package org.apache.hadoop.mapreduce
The org.apache.hadoop.mapreduce package
The Job class
