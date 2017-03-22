/**
 * Created by tanmayub on 3/18/17.
 */

import org.apache.commons.lang3.StringUtils;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class is a parser used in mapper class during pre processing
 */
public class HtmlParser {

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
    public ArrayList<String> process(String line) throws IOException {
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
        return new ArrayList<>(Arrays.asList(pageName, StringUtils.join(l, ",")));
    }
}