import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.*;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by tanmayub on 2/13/17.
 */

/**
 * This class is a mapper for pre process job
 */
public class PreprocessMap extends Mapper<Object, Text, NullWritable, PageRankDriver.PreprocessMapWritable> {
    HtmlParser hp = new HtmlParser();
    PageRankDriver.PreprocessMapWritable p = new PageRankDriver.PreprocessMapWritable();

    /**
     * This method emits an object with adjacency list and page name
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] parsed = hp.process(value.toString());
        //emit();
        p.pageName = parsed[0];
        p.adjList = parsed[1];
        if(parsed[0].indexOf("~") < 0 && !parsed[0].trim().isEmpty())
            context.write(NullWritable.get(), p);
    }
}

/**
 * This class is a parser used in mapper class during pre processing
 */
class HtmlParser {
    //private static HashMap<String, List<String>> hm = new HashMap<>();
    private static Pattern linkPattern;
    static {
        // Keep only html filenames ending relative paths and not containing tilde (~).
        linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
    }

    /**
     * this method gets a line from .bz2 file, parses it, gets links from within
     * div#bodyContent tag and return a pagename, list of links
     * @param line
     * @return
     * @throws IOException
     */
    public String[] process(String line) throws IOException {
        String pageName = line.substring(0,line.indexOf(":"));
        List<String> l = new ArrayList<>();
        if(pageName.indexOf("~") < 0) {
            Document doc = org.jsoup.Jsoup.parse(line);
            Element div = doc.select("div#bodyContent").first();
            Elements links = new Elements();
            if (div != null) {
                links = div.select("a[href]");
                //get page names from list of links
                for (int i = 0; i < links.size(); i++) {
                    String data = links.get(i).attributes().get("href");
                    data = data.replaceAll("%(?![0-9a-fA-F]{2})" , "%25");
                    data = data.replaceAll("\\+", "%2B");
                    String link = URLDecoder.decode(data, "UTF-8");
                    Matcher matcher = linkPattern.matcher(link);
                    if (matcher.find())
                        l.add(matcher.group(1));
                }
            }
        }
        return new String[]{pageName, StringUtils.join(l, ",")};
    }
}
