import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Created by Biyanta on 16/02/17.
 */
public class PageRankMain{
    static long total;
    static double delMain = -1.0;

    // user-defined counters
    public enum PAGE_COUNTER {
        total_records, Delta;
    }

    public static void main (String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Job 1 (Pre-processing the data)
        conf.setInt("total_records", 0);
        Job job1 = Job.getInstance(conf, "Pre processing");

        job1.setJarByClass(PageRankMain.class);

        job1.setMapperClass(PageRankPreProcess.PageRankMapper.class);
        job1.setReducerClass(PageRankPreProcess.PageRankReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        Path inputPath = new Path(args[0]);
        FileInputFormat.addInputPath(job1, inputPath);
        Path outPath = new Path(args[1]+"0" );
        FileOutputFormat.setOutputPath(job1, outPath);
        outPath.getFileSystem(conf).delete(outPath,true);

        job1.waitForCompletion(true);

        // Getting the total number of pages in the pre-processing output file
        total = job1.getCounters().findCounter(PAGE_COUNTER.total_records).getValue();

        int i = 0;
        for (i = 1; i <= 10; i++) {

            // job 2 ( Calculating page ranks for 10 iterations)
            conf.setInt("totalRecords", (int)total);
            conf.setDouble("delta", delMain);
            Job job2 = Job.getInstance(conf, "Page Rank Calculation");

            job2.setJarByClass(PageRankMain.class);

            job2.setMapperClass(PageRankCalculate.PageRankMapCalc.class);
            job2.setReducerClass(PageRankCalculate.PageRankReduceCalc.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            Path inPath = new Path(args[1]+(i-1) );
            FileInputFormat.addInputPath(job2, inPath);
            Path outputPath = new Path(args[1]+i);
            FileOutputFormat.setOutputPath(job2, outputPath);
            outputPath.getFileSystem(conf).delete(outputPath,true);

            job2.waitForCompletion(true);

            // the value of delta, to be passed to the map task in the next iteration
            delMain = Double.longBitsToDouble (job2.getCounters().findCounter(PAGE_COUNTER.Delta).getValue());

        }

        // job 3 (Final Delta calculation for last iteration)
        conf.setInt("totalRecords", (int)total);
        conf.setDouble("delta", delMain);

        Job job3 = Job.getInstance(conf, "Final Delta Calculation");
        job3.setJarByClass(PageRankMain.class);

        job3.setMapperClass(PageRankFinalDeltaCalc.PageRankDeltaCalc.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        // We are not calling any reducer here, so by default it will
        // set numReduceTasks to 1 and use the identity reducer

        Path inPath = new Path(args[1]+(i-1));
        FileInputFormat.addInputPath(job3, inPath);
        Path outputPath = new Path (args[1]+"FinalPageRank");
        FileOutputFormat.setOutputPath(job3,outputPath);
        outputPath.getFileSystem(conf).delete(outputPath,true);

        job3.waitForCompletion(true);

        // Job 4 (Final top k) to find the top 100 pages and displayed with their page ranks
        Job job4 = Job.getInstance(conf, "Top K calculation");

        job4.setJarByClass(PageRankMain.class);

        job4.setMapperClass(PageRankTopK.PageRankMapTopK.class);
        job4.setReducerClass(PageRankTopK.PageRankReduceTopK.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        Path finalIp = new Path (args[1]+"FinalPageRank");
        FileInputFormat.addInputPath(job4, finalIp);
        Path finalOp = new Path(args[1]);
        FileOutputFormat.setOutputPath(job4, finalOp);
        finalOp.getFileSystem(conf).delete(finalOp,true);

        System.exit(job4.waitForCompletion(true)? 0 : 1);

    }

}
