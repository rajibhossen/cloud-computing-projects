import java.io.*;

import java.util.Iterator;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
    long id;                   // the vertex ID
    Vector<Long> adjacent ;   // the vertex neighbors
    long centroid;  // centroid of the adjacent
    short depth;               // the BFS depth
    /* ... */
    Vertex ( long id,Vector<Long> adjacent,long centroid,short depth ) {
        this.id = id ;
        this.adjacent = adjacent;
        this.centroid = centroid;
        this.depth = depth;
    }

    public Vertex() { }

    public String toString(){
        return "ID: " + id + " centroid: " + centroid + " depth: " + depth;
    }

    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readLong();
        centroid = dataInput.readLong();
        depth = dataInput.readShort();
        int size = dataInput.readInt();

        adjacent=new Vector<Long>();
        for (int i = 0; i < size; i++)
        {
            adjacent.add(dataInput.readLong());
        }
    }
    public void write(DataOutput dataOutput) throws IOException {
        // TODO Auto-generated method stub
        dataOutput.writeLong(id);
        dataOutput.writeLong(centroid);
        dataOutput.writeShort(depth);
        int size = adjacent.size();
        dataOutput.writeInt(size);

        for (Long aLong : adjacent) {
            dataOutput.writeLong(aLong);
        }
    }
}

public class GraphPartition {
    static Vector<Long> centroids = new Vector<Long>();
    final static short max_depth = 8;
    static short BFS_depth = 0;
    static int count = 0;

    public static class Mapper1 extends Mapper<Object,Text,LongWritable,Vertex> {
        @Override
        public void map(Object key, Text value, Context context ) throws IOException, InterruptedException
        {
            Scanner input = new Scanner(value.toString()).useDelimiter(",");
            long centroid;
            long vertex_id = input.nextLong();
            Vector<Long> adjacent = new Vector<Long>();

            while (input.hasNext()){
                adjacent.add(input.nextLong());
            }
            if (count < 10){
                centroid = vertex_id;
            }
            else {
                centroid = -1;
            }
            count++;
            context.write(new LongWritable(vertex_id),new Vertex(vertex_id,adjacent,centroid,(short)0));
            input.close();

        }
    }
    public static class Mapper2 extends Mapper<LongWritable, Vertex, LongWritable, Vertex> {
        @Override
        public void map(LongWritable key, Vertex vertex, Context context) throws IOException, InterruptedException {
            Vector<Long> empty = new Vector<Long>();
            context.write(new LongWritable(vertex.id), vertex);
            if(vertex.centroid > 0) {
                for (long n : vertex.adjacent) {
                    context.write(new LongWritable(n), new Vertex(n, empty, vertex.centroid, BFS_depth));
                }
            }
        }
    }

    public static class Reducer2 extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {
        @Override
        public void reduce(LongWritable vertex_id, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {
            short min_depth = 1000;
            Vector<Long> emp = new Vector<Long>();
            Vertex m = new Vertex(vertex_id.get(), emp, -1, (short)(0)) ;
            for(Vertex v:values) {
                if(!(v.adjacent.isEmpty()))
                {
                    m.adjacent = v.adjacent;
                }
                if(v.centroid > 0 && v.depth < min_depth)
                {
                    min_depth = v.depth;
                    m.centroid = v.centroid;
                }
            }
            m.depth = min_depth;
            context.write(vertex_id, m);
        }

    }
    public static class Mapper3  extends Mapper <LongWritable, Vertex, LongWritable, IntWritable> {
        @Override
        public void map(LongWritable centroid,  Vertex value, Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(value.centroid),new IntWritable(1));
        }

    }
    public static class Reducer3 extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable>{
        @Override
        public void reduce(LongWritable centroid, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException
        {
            int m = 0;
            for (IntWritable v : values)
            {
                m = m + v.get();
            }
            context.write(centroid, new IntWritable(m));
        }
    }

    /* ... */

    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("Graph reading");

        job.setJarByClass(GraphPartition.class);
        job.setMapperClass(Mapper1.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        SequenceFileOutputFormat.setOutputPath(job,new Path(args[1] + "/i0"));

        /* ... First Map-Reduce job to read the graph */
        job.waitForCompletion(true);

        for ( short i = 0; i < max_depth; i++ ) {
            BFS_depth++;
            job = Job.getInstance();
            job.setJobName("Mapping Topography");
            job.setJarByClass(GraphPartition.class);

            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Vertex.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Vertex.class);

            job.setMapperClass(Mapper2.class);
            job.setReducerClass(Reducer2.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            SequenceFileInputFormat.setInputPaths(job, new Path(args[1] + "/i" + i));
            SequenceFileOutputFormat.setOutputPath(job,new Path(args[1]+ "/i" + (i+1)));

            job.waitForCompletion(true);
        }
        job = Job.getInstance();
        job.setJobName("Graph Size");
        job.setJarByClass(GraphPartition.class);

        job.setMapperClass(Mapper3.class);
        job.setReducerClass(Reducer3.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        SequenceFileInputFormat.setInputPaths(job, new Path(args[1] + "/i8"));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);
    }
}