import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Created by tanmayub on 1/28/17.
 */
public class InMapperCombiner {
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
            count = in.readDouble();
            isMin = in.readBoolean();
            value = in.readDouble();
        }

        public void write(DataOutput out) throws IOException{
            out.writeDouble(count);
            out.writeBoolean(isMin);
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

    public static class CombinerMapper extends Mapper<Object, Text, Text, InMapperCombiner.CompositeMapWritable> {
        private HashMap<Text, ArrayList<CompositeMapWritable>> hm = getMap();
        private Text stationid = new Text();

        public HashMap<Text, ArrayList<CompositeMapWritable>> getMap() {
            if(hm == null)
                hm = new HashMap<>();
            return hm;
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()) {
                String[] arr = itr.nextToken().split(",");
                if(arr[2].toLowerCase().equals("tmax") || arr[2].toLowerCase().equals("tmin")) {
                    stationid = new Text(arr[0]);
                    boolean isMin = arr[2].toLowerCase().equals("tmax") ? false : true;

                    ArrayList<CompositeMapWritable> c = new ArrayList<>();
                    if(hm.containsKey(stationid))
                        c = hm.get(stationid);
                    else {
                        c.add(new CompositeMapWritable(true, 0, 0));
                        c.add(new CompositeMapWritable(false, 0, 0));
                    }

                    if(isMin) {
                        c.get(0).count++;
                        c.get(0).value += Double.parseDouble(arr[3]);
                    }
                    else {
                        c.get(1).count++;
                        c.get(1).value += Double.parseDouble(arr[3]);
                    }

                    hm.put(stationid, c);
                }
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for(Text k : hm.keySet()) {
                ArrayList<CompositeMapWritable> l = hm.get(k);
                context.write(k, l.get(0));
                context.write(k, l.get(1));
            }
            hm.clear();
        }
    }

    public static class CombinerReducer extends Reducer<Text,InMapperCombiner.CompositeMapWritable,Text,InMapperCombiner.CompositeReduceWritable> {
        private InMapperCombiner.CompositeReduceWritable result = new InMapperCombiner.CompositeReduceWritable();

        public void reduce(Text key, Iterable<InMapperCombiner.CompositeMapWritable> values, Context context) throws IOException, InterruptedException {
            double sumMin = 0, sumMax = 0, ctMin = 0, ctMax = 0;
            for (InMapperCombiner.CompositeMapWritable val : values) {
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
        Job job = Job.getInstance(conf, "weather3");
        job.setJarByClass(InMapperCombiner.class);
        job.setMapperClass(InMapperCombiner.CombinerMapper.class);
        //job.setCombinerClass(InMapperCombiner.Combiner.class);
        job.setReducerClass(InMapperCombiner.CombinerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InMapperCombiner.CompositeMapWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
