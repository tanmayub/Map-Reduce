import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * Created by tanmayub on 1/28/17.
 */
public class SecondarySort {
    public static class MapKeyWritable implements Writable, WritableComparable<MapKeyWritable> {
        String stationid;
        String year;

        public MapKeyWritable() {}

        public void readFields(DataInput in) throws IOException {
            stationid = WritableUtils.readString(in);
            year = WritableUtils.readString(in);
        }

        public void write(DataOutput out) throws IOException{
            WritableUtils.writeString(out, stationid);
            WritableUtils.writeString(out, year);
        }

        public int compareTo(MapKeyWritable k) {
            if(this.stationid.equals(k.stationid))
                return this.year.compareTo(k.year);
            else
                return this.stationid.compareTo(k.stationid);
        }
    }

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
        String year;

        public void readFields(DataInput in) throws IOException{
            meanMinValue = in.readDouble();
            meanMaxValue = in.readDouble();
            year = WritableUtils.readString(in);
        }

        public void write(DataOutput out) throws IOException{
            out.writeDouble(meanMinValue);
            out.writeDouble(meanMaxValue);
            WritableUtils.writeString(out, year);
        }

        public String toString() {
            return "("+ this.year + ", " + this.meanMinValue + ", " + meanMaxValue + "), ";
        }
    }

    public static class SecondarySortReducerWritable implements Writable {
        ArrayList<CompositeReduceWritable> list = new ArrayList<>();

        public void readFields(DataInput in) throws IOException{
            for(CompositeReduceWritable c : list) {
                c.readFields(in);
            }
        }

        public void write(DataOutput out) throws IOException{
            for(CompositeReduceWritable c : list) {
                c.write(out);
            }
        }

        public String toString() {
            String res = "";
            for(CompositeReduceWritable c : list)
                res += c.toString();

            return "[" + res + "]";
        }
    }

    public static class CombinerMapper extends Mapper<Object, Text, MapKeyWritable, SecondarySort.CompositeMapWritable> {
        private SecondarySort.CompositeMapWritable val = new SecondarySort.CompositeMapWritable();
        private MapKeyWritable mapKey = new MapKeyWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()) {
                String[] arr = itr.nextToken().split(",");
                if(arr[2].toLowerCase().equals("tmax") || arr[2].toLowerCase().equals("tmin")) {
                    mapKey.stationid = arr[0];
                    mapKey.year = arr[1].substring(0,4);
                    val.isMin = arr[2].toLowerCase().equals("tmax") ? false : true;
                    val.count = 1;
                    val.value = Double.parseDouble(arr[3]);
                    context.write(mapKey, val);
                }
            }
        }
    }

    public static class FirstPartitioner extends Partitioner<MapKeyWritable, CompositeMapWritable>{
        @Override
        public int getPartition(MapKeyWritable key, CompositeMapWritable value, int numPartitions) {
            return Math.abs(key.stationid.hashCode()%numPartitions);
        }
    }

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(MapKeyWritable.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            MapKeyWritable ip1 = (MapKeyWritable) w1;
            MapKeyWritable ip2 = (MapKeyWritable) w2;
            return(ip1.compareTo(ip2));
        }
    }

    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(MapKeyWritable.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            MapKeyWritable ip1 = (MapKeyWritable) w1;
            MapKeyWritable ip2 = (MapKeyWritable) w2;
            return ip1.stationid.compareTo(ip2.stationid);
        }
    }

    public static class Combiner extends Reducer<MapKeyWritable,SecondarySort.CompositeMapWritable,MapKeyWritable,SecondarySort.CompositeMapWritable> {
        private SecondarySort.CompositeMapWritable result = new SecondarySort.CompositeMapWritable();

        public void reduce(MapKeyWritable key, Iterable<SecondarySort.CompositeMapWritable> values, Context context) throws IOException, InterruptedException {
            double sumMin = 0, sumMax = 0, ctMin = 0, ctMax = 0;
            for (SecondarySort.CompositeMapWritable val : values) {
                if(val.isMin) {
                    ctMin += val.count;
                    sumMin += val.value;
                }
                else {
                    ctMax += val.count;
                    sumMax += val.value;
                }
            }
            result = new SecondarySort.CompositeMapWritable(true, ctMin, sumMin);
            context.write(key, result);
            result = new SecondarySort.CompositeMapWritable(false, ctMax, sumMax);
            context.write(key, result);
        }
    }

    public static class CombinerReducer extends Reducer<MapKeyWritable,SecondarySort.CompositeMapWritable,Text,SecondarySort.SecondarySortReducerWritable> {
        private SecondarySort.SecondarySortReducerWritable result = new SecondarySort.SecondarySortReducerWritable();

        public void reduce(MapKeyWritable key, Iterable<SecondarySort.CompositeMapWritable> values, Context context) throws IOException, InterruptedException {
            double sumMin = 0, sumMax = 0, ctMin = 0, ctMax = 0;
            int year = -1;
            result = new SecondarySort.SecondarySortReducerWritable();
            for (SecondarySort.CompositeMapWritable val : values) {
                if(year < 0)
                    year = Integer.parseInt(key.year);
                if(!key.year.equalsIgnoreCase(year + "")) {
                    SecondarySort.CompositeReduceWritable res = new SecondarySort.CompositeReduceWritable();
                    res.meanMinValue = sumMin / ctMin;
                    res.meanMaxValue = sumMax / ctMax;
                    res.year = year + "";
                    year = Integer.parseInt(key.year);
                    result.list.add(res);
                    sumMax = 0;
                    sumMin = 0;
                    ctMax = 0;
                    ctMin = 0;
                }
                if (val.isMin) {
                    ctMin += val.count;
                    sumMin += val.value;
                } else {
                    ctMax += val.count;
                    sumMax += val.value;
                }
            }
            context.write(new Text(key.stationid), result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "weather4");
        job.setJarByClass(SecondarySort.class);
        job.setMapperClass(SecondarySort.CombinerMapper.class);
        job.setPartitionerClass(SecondarySort.FirstPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);
        job.setCombinerClass(SecondarySort.Combiner.class);
        job.setReducerClass(SecondarySort.CombinerReducer.class);
        job.setMapOutputKeyClass(MapKeyWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SecondarySort.CompositeMapWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
