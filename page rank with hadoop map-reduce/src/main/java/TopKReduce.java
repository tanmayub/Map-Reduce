import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by tanmayub on 2/14/17.
 */

/**
 * This class is a reducer for TopK calculations. uses a TreeMap which
 * is a RB tree and stores values according to descending order
 */
public class TopKReduce extends Reducer<NullWritable, Text, Text, Text> {

    private TreeMap<Double, Text> repToRecordMap = new TreeMap<Double, Text>();

    /**
     * This method will get dummy key and valueas pagename, page rank and adjacency list
     * This will consolidate all local maximums and sort them in descending order
     * and emit final ordered list with each having pagename, adjacency list, page rank
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(NullWritable key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {
        int k = Integer.valueOf(context.getConfiguration().getInt("Top K Elements", 10));
        for (Text value : values) {
            /*Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
                    .toString());*/
            String[] val = value.toString().split(":");
            repToRecordMap.put(Double.parseDouble(val[0]), new Text(val[1] + ":" + val[2]));

            /**
             * when a record is put, it there are more than k values in Treemap
             * it will remove k+1th value from map
             */
            if (repToRecordMap.size() > k) {
                repToRecordMap.remove(repToRecordMap.firstKey());
            }
        }

        //for each key in Treemap it will emit page name, adjacency list and page rank
        for (Double ky : repToRecordMap.descendingKeySet()) {
            String[] val = repToRecordMap.get(ky).toString().split(":");
            //val[0] is page name, val[1] is adjList, ky is pagerank
            context.write(new Text(val[0]), new Text(val[1] + ":" + ky));
        }
    }
}

