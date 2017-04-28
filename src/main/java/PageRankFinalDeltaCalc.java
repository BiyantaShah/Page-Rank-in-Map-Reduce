import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Biyanta on 22/02/17.
 */
// This class updates the page ranks for the final iteration.
// Since we update the value of page ranks (with delta) in the map of every iteration.
// For the last iteration, the value of delta obtained from the reduce won't be used to
// update the page ranks.
// So to get the correct page ranks , update the page ranks with delta value
// calculated in the reduce of the last iteration.
public class PageRankFinalDeltaCalc {

    static double alpha = 0.15;

    public static class PageRankDeltaCalc
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // fetching total records from counter
            int totalRecords = Integer.parseInt(context.getConfiguration().get("totalRecords"));

            String[] records = value.toString().split("\n");

            for (String record : records) {
                // Extracting page, out-link list and page ranks
                String page = record.substring(0, record.indexOf('[') - 1);
                String linkNames = record.substring(record.indexOf('[') + 1, record.indexOf(']'));
                String rank = record.substring(record.indexOf('{') + 1, record.indexOf('}'));

                String[] linkNameList = linkNames.split(", ");
                List<String> adjList = Arrays.asList(linkNameList);

                double pageRank = Double.parseDouble(rank);

                // updating the value of page ranks for all pages
                if (Double.parseDouble(context.getConfiguration().get("delta")) != -1.0) {
                    double d = Double.parseDouble(context.getConfiguration().get("delta"));
                    pageRank += (1 - alpha) * (d / totalRecords);
                }

                PageDetails pg1 = new PageDetails(adjList, pageRank);
                context.write(new Text(page), new Text (pg1.toString()));
            }
        }
    }
}
