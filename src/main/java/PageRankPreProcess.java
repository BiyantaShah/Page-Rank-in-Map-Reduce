import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by Biyanta on 21/02/17.
 */
// This class performs the pre-processing function; this includes, parsing the bz2 file
// and getting page names with its out-links. Also computing the dangling nodes.
public class PageRankPreProcess {

    public static class PageRankMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map (Object key, Text value, Context context) throws IOException, InterruptedException {

            try {
                // Parsing each line from Wiki parser and getting the pageName and linkNames for each line
                PageDetails pg = Bz2WikiParser.parseXML(value.toString());
                if (pg != null) {
                    // handles the condition when a page is a link in some other page
                    // but does not occur in the dataset.
                    for (String links : pg.linkNames) {
                        context.write(new Text(links), new Text());
                    }

                    // handles condition when a page is present in the dataset, but has
                    // an empty data set, so a dangling node.
                    context.write (new Text (pg.pageName), new Text ());
                    context.write(new Text(pg.pageName), new Text(pg.linkNames.toString()));
                }

            } catch (SAXException e) {
                e.printStackTrace();
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            }
        }
    }

    public static class PageRankReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String output = new String();

            for (Text str : values) {
                output = output + str.toString();
            }

            if (output.equals(""))
                output = "[]"; // "[]" indicates an empty adjacency list. Useful for recognizing dangling nodes.

            // updating counter for each record. Resulting in total number of pages
            context.getCounter(PageRankMain.PAGE_COUNTER.total_records).increment(1);

            // Adding a default dummy value for page rank,
            // which will be changed in the first iteration on calculating page rank
            output = output + " {-1.0}";

            context.write(key, new Text (output));
        }
    }
}
