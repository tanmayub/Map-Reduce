import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by tanmayub on 2/13/17.
 */
public class HtmlParser1 {
    private static HashMap<String, List<String>> hm = new HashMap<>();
    private static Pattern linkPattern;
    static {
        // Keep only html filenames ending relative paths and not containing tilde (~).
        linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
    }
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("Input bz2 file required on command line.");
            System.exit(1);
        }
        String url = args[0];
        BufferedReader reader = null;
        try {
            File inputFile = new File(url);
            if (!inputFile.exists() || inputFile.isDirectory() || !inputFile.getName().endsWith(".bz2")) {
                System.out.println("Input File does not exist or not bz2 file: " + url);
                System.exit(1);
            }
            BZip2CompressorInputStream inputStream = new BZip2CompressorInputStream(new FileInputStream(inputFile));
            reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while((line = reader.readLine()) != null) {
                String pageName = line.substring(0,line.indexOf(":"));
                if(pageName.indexOf("~") < 0) {
                    Document doc = Jsoup.parse(line);
                    Element div = doc.select("div#bodyContent").first();
                    Elements links = new Elements();
                    List<String> l = new ArrayList<>();
                    if (div != null) {
                        links = div.select("a[href]");
                        //get page names from list of links
                        for (int i = 0; i < links.size(); i++) {
                            String link = URLDecoder.decode(links.get(i).attributes().get("href"), "UTF-8");
                            Matcher matcher = linkPattern.matcher(link);
                            if (matcher.find())
                                l.add(matcher.group(1));
                        }
                    }
                    hm.put(pageName, l);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try { reader.close(); } catch (IOException e) {}
        }
        for(String k : hm.keySet()) {
            System.out.println(k + ":[" + StringUtils.join(hm.get(k), ",") + "]");
        }
        System.out.println(hm.size());
    }
}
