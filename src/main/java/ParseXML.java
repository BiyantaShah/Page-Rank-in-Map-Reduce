//import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
//import org.apache.hadoop.io.compress.bzip2.Bzip2Compressor;
//import org.jsoup.Jsoup;
//import org.jsoup.nodes.Document;
//
//import java.io.*;
//
///**
// * Created by Biyanta on 23/02/17.
// */
//public class ParseXML {
//
//    // To parse in a human readable format. "readable.txt" will have the records in a human readable format 
//    static void printInNormalFormat(File ipFile) throws IOException {
//
//        BufferedReader reader = null;
//        BZip2CompressorInputStream inputStream = new BZip2CompressorInputStream(new FileInputStream(ipFile));
//        reader = new BufferedReader(new InputStreamReader((inputStream)));
//        BufferedWriter writer = new BufferedWriter(new FileWriter("readable.txt"));
//        String line;
//        while ((line = reader.readLine()) != null) {
//            Document doc = Jsoup.parse(line);
//            writer.write(String.valueOf(doc));
//        }
//        writer.close();
//    }
//
//    public static void main (String[] args) throws IOException {
//
//        printInNormalFormat(inputFile);
//    }
//
//
//}
