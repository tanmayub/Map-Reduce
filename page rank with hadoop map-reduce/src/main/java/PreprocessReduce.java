import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by tanmayub on 2/14/17.
 */

/**
 * This class is a reducer for preprocessing
 */
public class PreprocessReduce extends Reducer<NullWritable,PageRankDriver.PreprocessMapWritable,Text,Text> {
    HashMap<String,String> hm = new HashMap<>();

    /**
     * This method accepts an object with pagename and adjacency list and
     * emits a line with key as page name and value as adjacency list and initial page rank
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(NullWritable key, Iterable<PageRankDriver.PreprocessMapWritable> values, Context context) throws IOException, InterruptedException {
        for(PageRankDriver.PreprocessMapWritable o : values) {
            hm.put(o.pageName + ":", "[" + o.adjList + "]");
            for(String s : o.adjList.split(",")) {
                hm.put(s + ":", "[]");
            }
        }

        //hs has all nodes including dangling nodes
        for(String k : hm.keySet()) {
            String pgRank = String.valueOf(1/(double)hm.size());
            context.write(new Text(k), new Text (hm.get(k) + ":" + pgRank));
        }
    }
}
