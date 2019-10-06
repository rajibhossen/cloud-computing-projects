import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.Scanner;
import java.util.Vector;


class Point implements WritableComparable<Point> {
    double x;
    double y;

    Point(double x, double y){
        this.x = x;
        this.y = y;
    }

    Point() {
        this.x = 0;
        this.y = 0;
    }

    public int compareTo(Point point){
        if (this.x == point.x){
            return Double.compare(this.y, point.y);
        }
        else if(this.x < point.x)
        {
            return -1;
        }
        else {
            return 1;
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(x);
        dataOutput.writeDouble(y);
    }

    public void readFields(DataInput dataInput) throws IOException {
        x = dataInput.readDouble();
        y = dataInput.readDouble();
    }
    public String toString(){
        return this.x + "," + this.y;
    }
}

public class KMeans {
    private static Vector<Point> centroids = new Vector<Point>(100);

    public static class AvgMapper extends Mapper<Object,Text,Point,Point> {

        public void setup(Context context) throws IOException {
            URI[] paths = context.getCacheFiles();
            Configuration configuration = context.getConfiguration();
            FileSystem fileSystem = FileSystem.get(configuration);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(paths[0]))));

            String centroid = reader.readLine();
            while (centroid != null){
                String[] ind_points = centroid.split(",");
                Point point = new Point(Double.parseDouble(ind_points[0]), Double.parseDouble(ind_points[1]));
                //System.out.println(point);
                centroids.add(point);

                // continue reading centroids from file
                centroid = reader.readLine();
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            Point input = new Point(s.nextDouble(), s.nextDouble());
            double closest = Double.MAX_VALUE;
            Point closest_point = new Point();
            for (Point centroid : centroids) {
                double distance = Math.sqrt((centroid.x - input.x) * (centroid.x - input.x) + (centroid.y - input.y) * (centroid.y - input.y));
                if(distance < closest){
                    closest = distance;
                    closest_point = centroid;
                }
            }
            context.write(closest_point, input);
        }

    }

    public static class AvgReducer extends Reducer<Point,Point,Point,Object> {
        public void reduce(Point centroid, Iterable<Point> points, Context context) throws IOException, InterruptedException {
            int count = 0;
            double SX = 0.0;
            double SY = 0.0;

            for(Point p:points){
                count++;
                SX += p.x;
                SY += p.y;
            }
            centroid.x = SX / count;
            centroid.y = SY / count;

            context.write(centroid, null);
        }

    }

    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("MapReduceKMeansJob");
        job.setJarByClass(KMeans.class);
        job.addCacheFile(new URI(args[1]));

        job.setOutputKeyClass(Point.class);
        job.setOutputValueClass(Point.class);
        job.setMapOutputKeyClass(Point.class);
        job.setMapOutputValueClass(Point.class);
        job.setMapperClass(AvgMapper.class);
        job.setReducerClass(AvgReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);

   }
}
