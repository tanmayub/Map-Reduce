import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by tanmayub on 2/14/17.
 */

/**
 * This is a mapper class for calculating topK elements. Used inmapper combiner
 * to reduce data flow from mapper to reducer
 */
public class TopKMap extends Mapper<Object, Text, NullWritable, Text> {
    // Our output key and value Writables
    private TreeMap<Double, Text> repToRecordMap = new TreeMap<Double, Text>();

    /**
     * This method will read value as pagename, adjacency list, pagerank
     * I have used treemap here, which is a RB tree and stores values according to
     * descending order
     * This method calculates all local maximums
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        int k = Integer.valueOf(context.getConfiguration().getInt("Top K Elements", 10));
        // Parse the input string into a nice map
        String[] arr = value.toString().split(":");
        /*Map<String, Double> parsed = new Map<>();
        if (parsed == null) {
            return;
        }

        String userId = parsed.get("Id");
        String reputation = parsed.get("Reputation");*/
        String pageName = arr[0].trim();
        String adjList = arr[1];
        double pageRank = Double.parseDouble(arr[2]);

        repToRecordMap.put(pageRank, new Text(pageName + ":" + adjList));

        /**
         * when a record is put, it there are more than k values in Treemap
         * it will remove k+1th value from map
         */
        if (repToRecordMap.size() > k) {
            repToRecordMap.remove(repToRecordMap.firstKey());
        }
    }

    /**
     * In-mapper combining. for each key in treemap emits dmmy node as key and
     * value as pagerank, adjacency list, page name
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        for (Double k : repToRecordMap.keySet()) {
            Text val = new Text(k + ":" + repToRecordMap.get(k));
            context.write(NullWritable.get(), val);
        }
    }
}