import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
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
public class CombinerProg {

    public static class CompositeMapWritable implements Writable {
        boolean isMin;
        double count;
        double value;

        public CompositeMapWritable() {

        }

        public CompositeMapWritable(boolean isMin, double count, double value){
            this.isMin = isMin;
            this.count = count;
            this.value = value;
        }

        public void readFields(DataInput in) throws IOException {
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

    public static class CombinerMapper extends Mapper<Object, Text, Text, CombinerProg.CompositeMapWritable> {
        private CombinerProg.CompositeMapWritable val = new CombinerProg.CompositeMapWritable();
        private Text station = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()) {
                String[] arr = itr.nextToken().split(",");
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

    public static class Combiner extends Reducer<Text,CombinerProg.CompositeMapWritable,Text,CombinerProg.CompositeMapWritable> {
        private CombinerProg.CompositeMapWritable result = new CompositeMapWritable();

        public void reduce(Text key, Iterable<CombinerProg.CompositeMapWritable> values, Context context) throws IOException, InterruptedException {
            double sumMin = 0, sumMax = 0, ctMin = 0, ctMax = 0;
            for (CombinerProg.CompositeMapWritable val : values) {
                if(val.isMin) {
                    ctMin += val.count;
                    sumMin += val.value;
                }
                else {
                    ctMax += val.count;
                    sumMax += val.value;
                }
            }
            result = new CombinerProg.CompositeMapWritable(true, ctMin, sumMin);
            context.write(key, result);
            result = new CombinerProg.CompositeMapWritable(false, ctMax, sumMax);
            context.write(key, result);
        }
    }

    public static class CombinerReducer extends Reducer<Text,CombinerProg.CompositeMapWritable,Text,CombinerProg.CompositeReduceWritable> {
        private CombinerProg.CompositeReduceWritable result = new CombinerProg.CompositeReduceWritable();

        public void reduce(Text key, Iterable<CombinerProg.CompositeMapWritable> values, Context context) throws IOException, InterruptedException {
            double sumMin = 0, sumMax = 0, ctMin = 0, ctMax = 0;
            for (CombinerProg.CompositeMapWritable val : values) {
                if(val.isMin) {
                    ctMin += val.count;
                    sumMin += val.value;
                }
                else {
                    ctMax += val.count;
                    sumMax += val.value;
                }
            }
            result.meanMinValue = sumMin / ctMin;
            result.meanMaxValue = sumMax / ctMax;
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "weather2");
        job.setJarByClass(CombinerProg.class);
        job.setMapperClass(CombinerProg.CombinerMapper.class);
        job.setCombinerClass(CombinerProg.Combiner.class);
        job.setReducerClass(CombinerProg.CombinerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CombinerProg.CompositeMapWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
