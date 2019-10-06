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
import java.util.Hashtable;
import java.util.Scanner;
import java.util.Set;
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

class Avg implements Writable{
    double sumX;
    double sumY;
    long count;

    Avg(double x, double y, long i) {
        sumX = x;
        sumY = y;
        count = i;
    }
    public Avg(){
        this.sumX = 0;
        this.sumY = 0;
        this.count = 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(sumX);
        dataOutput.writeDouble(sumY);
        dataOutput.writeLong(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        sumX = dataInput.readDouble();
        sumY = dataInput.readDouble();
        count = dataInput.readLong();
    }
}

public class KMeans {
    private static Vector<Point> centroids = new Vector<Point>(100);
    private static Hashtable<Point, Avg> table;

    public static class AvgMapper extends Mapper<Object,Text,Point,Avg> {

        public void setup(Context context) throws IOException {
            table = new Hashtable<Point, Avg>();

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

        public void cleanup(Context context) throws IOException, InterruptedException{
            Set<Point> keys = table.keySet();
            for (Point key: keys) context.write(key, table.get(key));
            table.clear();

        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            Point input = new Point(s.nextDouble(), s.nextDouble());
            double closest = Double.MAX_VALUE;
            Point closest_point = new Point();
            int count = 0;
            for (Point centroid : centroids) {
                count++;
                double distance = Math.sqrt((centroid.x - input.x) * (centroid.x - input.x) + (centroid.y - input.y) * (centroid.y - input.y));
                if(distance < closest){
                    closest = distance;
                    closest_point = centroid;

                    if (table.get(closest_point) == null){
                        table.put(closest_point, new Avg(input.x, input.y, 1));
                    }
                    else {
                        table.put(closest_point,
                                new Avg(table.get(closest_point).sumX + input.x,
                                        table.get(closest_point).sumY + input.y,
                                        table.get(closest_point).count + 1));
                    }
                }
            }
            //context.write(closest_point, input);
        }


    }

    public static class AvgReducer extends Reducer<Point,Avg,Point,Object> {
        public void reduce(Point centroid, Iterable<Avg> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            double SX = 0.0;
            double SY = 0.0;

            for(Avg a:values){
                count += a.count;
                SX += a.sumX;
                SY += a.sumY;
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
        job.setOutputValueClass(Object.class);

        job.setMapOutputKeyClass(Point.class);
        job.setMapOutputValueClass(Avg.class);

        job.setMapperClass(AvgMapper.class);
        job.setReducerClass(AvgReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);

   }
}
