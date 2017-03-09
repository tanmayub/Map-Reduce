import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by tanmayub on 1/27/17.
 */

/**
 * Summary: This is main class which has main method calling mapper and reducer
 */
public class NoCombiner {

    /**
     * summary: This class is used as value output for mapper
     * binds count, isMin, value in one object.
     */
    public static class CompositeMapWritable implements Writable {
        boolean isMin;
        double count;
        double value;

        public void readFields(DataInput in) throws IOException{
            isMin = in.readBoolean();
            count = in.readDouble();
            value = in.readDouble();
        }

        public void write(DataOutput out) throws IOException{
            out.writeBoolean(isMin);
            out.writeDouble(count);
            out.writeDouble(value);
        }
    }

    /**
     * summary: This class is used as value output for reducer
     * binds meanMinimum value and meanMaximum value in one object.
     */
    public static class CompositeReduceWritable implements Writable {
        double meanMinValue;
        double meanMaxValue;

        public void readFields(DataInput in) throws IOException{
            meanMinValue = in.readDouble();
            meanMaxValue = in.readDouble();
        }

        public void write(DataOutput out) throws IOException{
            out.writeDouble(meanMinValue);
            out.writeDouble(meanMaxValue);
        }

        public String toString() {
            return this.meanMinValue + "," + meanMaxValue;
        }
    }

    /**
     * summary: Mapper class
     * input: Object, Text
     * output: Text as key and CompositeMapWritable class object as value
     */
    public static class NoCombinerMapper extends Mapper<Object, Text, Text, CompositeMapWritable> {
        private CompositeMapWritable val = new CompositeMapWritable();
        private Text station = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()) {
                String[] arr = itr.nextToken().split(",");
                //if temperature types are tmin/tmax create a CompositeMapWritable object and emit
                if(arr[2].toLowerCase().equals("tmax") || arr[2].toLowerCase().equals("tmin")) {
                    station.set(arr[0]);
                    val.isMin = arr[2].toLowerCase().equals("tmax") ? false : true;
                    val.count = 1;
                    val.value = Double.parseDouble(arr[3]);
                    context.write(station, val);
                }
            }
        }
    }

    /**
     * summary: reducer class
     */
    public static class NoCombinerReducer extends Reducer<Text,CompositeMapWritable,Text,CompositeReduceWritable> {
        private CompositeReduceWritable result = new CompositeReduceWritable();

        public void reduce(Text key, Iterable<CompositeMapWritable> values, Context context) throws IOException, InterruptedException {
            double sumMin = 0, sumMax = 0, ctMin = 0, ctMax = 0;
            for (CompositeMapWritable val : values) {
                //if value in list is min- add count and value to min
                if(val.isMin) {
                    ctMin += val.count;
                    sumMin += val.value;
                }
                else {//else add count and value to max
                    ctMax += val.count;
                    sumMax += val.value;
                }
            }
            //clculate MeanMinimum and MeanMaximum values
            result.meanMinValue = sumMin / ctMin;
            result.meanMaxValue = sumMax / ctMax;
            context.write(key, result);
        }
    }

    /**
     * Main class-- creates job and executes it
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "weather1");
        job.setJarByClass(NoCombiner.class);
        job.setMapperClass(NoCombinerMapper.class);
        //job.setCombinerClass(NoCombinerReducer.class);
        job.setReducerClass(NoCombinerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CompositeMapWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
