import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

/**
 * Created by Biyanta on 21/02/17.
 */
// This class finds the 100 highest page ranks and displays
// them in descending order with the page names.
public class PageRankTopK {

    public static class PageRankMapTopK
            extends Mapper<Object, Text, Text, Text> {

        // Data structure to store top 100 page ranks with their page names
        TreeMap<Double, String> topKMap;

        public void setup(Context context) {
            topKMap = new TreeMap<>();
        }

        public void map(Object key, Text value, Context context) {
            String[] records = value.toString().split("\n");

            for (String record : records) {
                String page = record.substring(0, record.indexOf('[') - 1);
                String pageRank = record.substring(record.indexOf('{') + 1, record.indexOf('}'));

                topKMap.put(Double.parseDouble(pageRank), page);
                // if two pages have the exact same page rank, then the previous page
                // will be over written and will not appear in top 100 list.
                // However, the possibility of that happening in a practical scenario is very unlikely
                // I had a talk TA Ankur Shanbaug and he advised to document this discrepancy.
                // The solution for this could be to check whether the page rank is already in the map.
                // If it is then we can append the incoming page name to the already present page name.
                // This way for same page ranks, we will have its page names listed.

                if (topKMap.size() > 100) {
                    topKMap.remove(topKMap.firstKey());
                }
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (double rank : topKMap.keySet()) {
                String output = Double.toString(rank) + ", " + topKMap.get(rank);
                context.write(new Text ("dummy"), new Text (output));
            }
        }
    }

    public static class PageRankReduceTopK
            extends Reducer<Text, Text, Text, Text> {

        // global top K map
        TreeMap<Double, String> topKMapFinal = new TreeMap<>();

        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {

            for (Text value : values) {

                String record = value.toString();
                String [] pageDetails = record.split(", ");

                topKMapFinal.put (Double.parseDouble(pageDetails[0]), pageDetails[1]);

                if (topKMapFinal.size() > 100) {
                    topKMapFinal.remove(topKMapFinal.firstKey());
                }
            }

            for (Double rank : topKMapFinal.descendingMap().keySet()) {
                context.write(new Text(topKMapFinal.get(rank)), new Text (Double.toString(rank)));
            }
        }

    }

}
