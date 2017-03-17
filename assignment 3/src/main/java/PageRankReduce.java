import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by tanmayub on 2/13/17.
 */

/**
 * This class is a reducer for PgeRank algorithm
 */
public class PageRankReduce extends Reducer<Text,PageRankDriver.CompositeMapWritable,Text,Text> {
    /**
     * This method will collect key as page name and all its pageranks
     * emitted by mapper. This will emit key as page rank and adjacencylist, pagerank asvalue
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<PageRankDriver.CompositeMapWritable> values, Context context) throws IOException, InterruptedException {
        double recordCount = Double.valueOf(context.getConfiguration().getLong("Global record counter", 1));
        double alpha = Double.valueOf(context.getConfiguration().getFloat("Global alpha", 0));
        PageRankDriver.CompositeMapWritable c = new PageRankDriver.CompositeMapWritable();
        //if() {//(!key.toString().equals(context.getConfiguration().get("dummy"))) { //not a dummy node- emit values regular for node
        double sum = 0;
        for (PageRankDriver.CompositeMapWritable o : values) {
            if (o.isNode) {
                //node found, filling up object c
                /**
                 * if adjacency list is empty- it is a dangling node-
                 * updating delta value and saving node object
                 */
                if(o.adjList.isEmpty()) {
                    double delta = Double.valueOf(context.getConfiguration().getFloat("Global delta", 0));
                    delta += o.pageRank / recordCount;

                    context.getCounter(PageRankDriver.MyCounter.counter).setValue(Double.doubleToLongBits(delta));
                }
                c = new PageRankDriver.CompositeMapWritable(o);
            } else {
                //not a node-- change only pagerank of the node
                sum += o.pageRank;
            }
        }
        String pgRank = String.valueOf(alpha / recordCount + (1 - alpha) * sum);
        context.write(new Text(key.toString() + ":"), new Text(  "[" + c.adjList + "]" + ":" + pgRank));
    }
}
