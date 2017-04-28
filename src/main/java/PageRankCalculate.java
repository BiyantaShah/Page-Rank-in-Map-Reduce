import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;


/**
 * Created by Biyanta on 21/02/17.
 */
// This class runs for 10 iterations calculating page ranks, along with the dangling node mass.
public class PageRankCalculate {

    // I have chosen alpha = 0.15. Choosing alpha = 0.5, makes the jumping to pages of a
    // half and half probability (will generate like a sinusodial wave). By restricting the upper value, makes the
    // graph more smoother and nearer to the x-axis, thus nearly converging it with the x-axis.
    // You can even see it with the results.
    static double alpha = 0.15;

    public static class PageRankMapCalc
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // fetching total records from counter
            int totalRecords = Integer.parseInt(context.getConfiguration().get("totalRecords"));

            String[] records = value.toString().split("\n");
            for (String record : records) {
                // Extracting value of the page name, its out-links list and page rank
                // using the delimiters, from the input file
                String page = record.substring(0, record.indexOf('[') - 1);
                String linkNames = record.substring(record.indexOf('[')+1, record.indexOf(']'));
                String rank = record.substring(record.indexOf('{') + 1, record.indexOf('}'));

                String[] linkNameList = linkNames.split(", ");
                // here I am joining the data by ", ". On further observation,
                // I saw that there were a few page names which had this delimiter.
                // So I changed the delimiter to "~" since we are removing all the pages with "~".
                // I ran it with these changes on the local machine and didn't observe major changes
                // in the final output. As I already had run my code with ", " delimiter on AWS
                // by the time I discovered the delimiter discrepancy , I did not run again on AWS EMR

                List<String> adjList = Arrays.asList(linkNameList);

                double pageRank = Double.parseDouble(rank);

                // setting the initial page rank for all pages
                if (pageRank == -1.0)
                    pageRank = 1.0/totalRecords;

                // if delta is set to a value, then update the page rank. Default value of delta is 0.0
                if ( Double.parseDouble(context.getConfiguration().get("delta")) != -1.0) {
                    double d = Double.parseDouble(context.getConfiguration().get("delta"));
                    pageRank += (1-alpha) * (d/totalRecords);
                }

                PageDetails pg1 = new PageDetails(adjList, pageRank);

                context.write(new Text(page), new Text (pg1.toString()));

                // delta = Σ (page rank of dangling nodes). When we encounter a
                // dangling node, we emit the key as "dummy" to get them together in the reducer
                // where the summation takes place
                if (adjList.size() == 1 && adjList.get(0).equals(""))
                    context.write (new Text ("dummy"), new Text (Double.toString(pageRank)));
                else {
                    // to calculate Σ(P(m)/C(m)) for m ∈ L(n), where L(n) are links pointing to a page n
                    for (String link : linkNameList) {
                        double outLinkRank = pageRank/linkNameList.length;
                        context.write(new Text(link), new Text(Double.toString(outLinkRank)));
                    }
                }
            }
        }
    }

    public static class PageRankReduceCalc
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // fetching total records from counter
            int totalRecords = Integer.parseInt(context.getConfiguration().get("totalRecords"));

            double s = 0.0;
            double danglingMass = 0.0;
            PageDetails Page = null;

            if (key.toString().equals("dummy")){

                for (Text str : values) {
                    String rank = str.toString();
                    danglingMass += Double.parseDouble(rank);

                    //setting the counter Delta to the value obtained
                    context.getCounter(PageRankMain.PAGE_COUNTER.Delta).
                            setValue(Double.doubleToLongBits(danglingMass));
                }
            }
            else {
                for (Text str : values) {
                    String record = str.toString();
                    try {
                        // A page rank contribution from an in-link was found:
                        // add it to the running sum
                        double rank = Double.parseDouble(record);
                        s += rank;

                    } catch(NumberFormatException e){

                        // If we encounter a page, recover the page structure
                        // (since we have the key as the page name, get the out-link list and page rank)
                        String linkNames = record.substring(record.indexOf('[')+1, record.indexOf(']'));
                        String [] linkNameList = linkNames.split(", ");
                        String pageRank = record.substring(record.indexOf('{') + 1, record.indexOf('}'));
                        PageDetails pg = new PageDetails(Arrays.asList(linkNameList), Double.parseDouble(pageRank));
                        Page = new PageDetails (pg.linkNames, pg.pageRank);
                    }
                }

                // updating the page rank as per formula (without dangling mass)
                Page.pageRank = (alpha/totalRecords) + (1-alpha) * (s);

                context.write(key, new Text (Page.toString()));
            }

        }

    }
}
