import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by tanmayub on 2/14/17.
 */

/**
 * This class is a map only task kicked at the end of last reduce call for
 * PageRank algorithm to correct page ranks for the last run.
 */
public class PostprocessMap extends Mapper<Object, Text, Text, Text> {
    /**
     * This method accepts vale as pagename, pagerank, adjacency list
     * and will emit page name as key and adjacency list, pagerank as value
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        PageRankDriver.CompositeMapWritable c = new PageRankDriver.CompositeMapWritable();
        double alpha = Double.valueOf(context.getConfiguration().getFloat("Global alpha", 0));
        String[] arr = value.toString().split(":");
        String strAdjList = arr[1].trim().substring(arr[1].indexOf("["), arr[1].indexOf("]") - 1);
        c = new PageRankDriver.CompositeMapWritable(true, strAdjList, Double.parseDouble(arr[2]) + (1 - alpha) * Double.valueOf(context.getConfiguration().getFloat("Global delta", 0)));
        Text keyNode = new Text(arr[0].trim() + ":");
        if (!keyNode.toString().isEmpty()) {
            context.write(keyNode, new Text(  "[" + c.adjList + "]" + ":" + String.valueOf(c.pageRank)));
        }
    }
}
