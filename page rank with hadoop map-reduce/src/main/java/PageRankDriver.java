import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

/**
 * Created by tanmayub on 2/13/17.
 */

/**
 * This is the driver program class for page rank calculation
 */
public class PageRankDriver {
    /**
     * This counter is used to get delta value from reduce call,
     * to be provided in next map task, to correct page ranks
     */
    enum MyCounter{
        counter
    }

    /**
     * This class is used as value output for mapper while running PageRank algorithm
     * contains isNode, pageRank, adjList
     */
    public static class CompositeMapWritable implements Writable {
        boolean isNode;
        double pageRank;
        String adjList = "";

        public CompositeMapWritable() {

        }

        public CompositeMapWritable(boolean isNode, String adjList, double pageRank) {
            this.adjList = adjList;
            this.isNode = isNode;
            this.pageRank = pageRank;
        }

        public CompositeMapWritable(CompositeMapWritable c) {
            this.adjList = c.adjList;
            this.isNode = c.isNode;
            this.pageRank = c.pageRank;
        }

        public void readFields(DataInput in) throws IOException {
            pageRank = in.readDouble();
            isNode = in.readBoolean();
            adjList = WritableUtils.readString(in);
        }

        public void write(DataOutput out) throws IOException{
            out.writeDouble(pageRank);
            out.writeBoolean(isNode);
            WritableUtils.writeString(out, adjList);
        }
    }

    /**
     * This class is used as value output for mapper in pre-processing part(parsing .bz file)
     * contains pageName, adjList
     */
    public static class PreprocessMapWritable implements Writable {
        String pageName;
        String adjList;

        public PreprocessMapWritable() {

        }

        public void readFields(DataInput in) throws IOException {
            pageName = WritableUtils.readString(in);
            adjList = WritableUtils.readString(in);
        }

        public void write(DataOutput out) throws IOException{
            WritableUtils.writeString(out, pageName);
            WritableUtils.writeString(out, adjList);
        }
    }

    /**
     * main method
     * drives the flow
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        /*preprocess data- calculate tuples- pagename, adjacency list, initial page rank(1/total nodes)*/
        long reduceRecordCounter = preProcess(args);

        //call a new map reduce job to run page rank algorithm on
        //output produced by above map job
        double delta = 0;
        double alpha = 0.2;
        //long reduceRecordCounter = 4;
        FileUtils.deleteQuietly(new File(args[1] + "//_SUCCESS"));
        String input = args[1];
        String output = args[1] + "0";
        int i = 0;
        for(i = 0; i < 10; i++) {
            Configuration conf = new Configuration();
            //conf.set(), conf.get() to set global counters
            conf.setFloat("Global delta", (float)delta);
            conf.setFloat("Global alpha", (float)alpha);
            conf.setLong("Global record counter", reduceRecordCounter);
            Job job = Job.getInstance(conf, "pageRank2");
            job.setJarByClass(PageRankDriver.class);
            job.setMapperClass(PageRankMap.class);
            job.setReducerClass(PageRankReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setMapOutputValueClass(PageRankDriver.CompositeMapWritable.class);
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            job.waitForCompletion(true);
            delta = Double.longBitsToDouble(job.getCounters().findCounter(MyCounter.counter).getValue());
            //moveOutputToInput(args, "part-r-00000");
            FileUtils.deleteQuietly(new File(output + "//_SUCCESS"));
            input = output;
            output = output.substring(0 , output.length() - 1) + (i + 1);
        }
        //Map only task to correct PR
        args[0] = input;
        args[1] = output;
        FileUtils.deleteQuietly(new File(output + "//_SUCCESS"));
        postProcess(args, delta, alpha);

        //top-k records
        //Citation: Top K code is a variation of Top10 from module
        FileUtils.deleteQuietly(new File(output + "//_SUCCESS"));
        args[0] = output;
        args[1] = output.substring(0, output.length() - 1) + (i + 1);
        selectTopKRecords(args, 100);
    }

    /**
     * This method creates and runs job for TopK results
     * @param args
     * @param numRecords
     * @throws Exception
     */
    public static void selectTopKRecords(String[] args, int numRecords) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: TopTenDriver <in> <out>");
            System.exit(2);
        }

        conf.setInt("Top K Elements", numRecords);
        Job job = new Job(conf, "Top K Users by Reputation");
        job.setJarByClass(PageRankDriver.class);
        job.setMapperClass(TopKMap.class);
        job.setReducerClass(TopKReduce.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.waitForCompletion(true);
    }

    /**
     * This method creates and runs last map only task to correct values of
     * page rank
     * @param args
     * @param delta
     * @param alpha
     * @throws Exception
     */
    //last map only task which runs to add delta values for last run
    public static void postProcess(String[] args, double delta, double alpha) throws Exception{
        Configuration conf = new Configuration();
        conf.setFloat("Global delta", (float)delta);
        conf.setFloat("Global alpha", (float)alpha);
        Job job = Job.getInstance(conf, "pageRank3");
        job.setJarByClass(PageRankDriver.class);
        job.setMapperClass(PostprocessMap.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

    /**
     * creates and runs map-reduce job for preprocessing file
     * @param args
     * @return
     * @throws Exception
     */
    //it calls a mapper which then calls the parser
    //mapper emits each line from parser-- output file is created
    //copy that file to input folder for processing of map-reduce
    public static long preProcess(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "pageRank1");
        job.setJarByClass(PageRankDriver.class);
        job.setMapperClass(PreprocessMap.class);
        job.setReducerClass(PreprocessReduce.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PageRankDriver.PreprocessMapWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
        return job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();
    }
}
