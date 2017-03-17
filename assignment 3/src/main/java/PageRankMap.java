import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by tanmayub on 2/13/17.
 */

/**
 * This class is a mapper for running PageRank algorithm
 */
public class PageRankMap extends Mapper<Object, Text, Text, PageRankDriver.CompositeMapWritable> {
    /**
     * This method accepts a value as page name, adjacency list, page rank
     * and emits pagename as key and object with pagerank,adjacency list and isNode flag
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
        Text keyNode = new Text(arr[0].trim());
        /*
        emits the node and iterates over its adjacency list to emit all nodes in adjacency list
        with updated pageranks
         */
        if(!keyNode.toString().isEmpty()) {
            context.write(keyNode, c);

            if (!strAdjList.isEmpty()) { //not dangling node
                String[] adjList = strAdjList.split(",");
                double p = c.pageRank / (double) adjList.length;
                for (String s : adjList) {
                    c.isNode = false;
                    c.adjList = new String();
                    c.pageRank = p;
                    context.write(new Text(s), c);
                }
            }
        }
    }
}
