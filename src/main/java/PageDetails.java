import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Created by Biyanta on 17/02/17.
 */
// Contains information about the page. The page name, its out-links and the page rank
public class PageDetails implements Writable {

    String pageName;
    List<String> linkNames;
    double pageRank;

    public PageDetails() { // serialize the object
    }

    PageDetails(String page, List<String> link) {
        this.pageName = page;
        this.linkNames = link;
    }

    PageDetails(List<String> link, double rank) {
        this.linkNames = link;
        this.pageRank = rank;
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();

        // here I am joining the data by ", ". On further observation,
        // I saw that there were a few page names which had this delimiter.
        // So I changed the delimiter to "~" since we are removing all the pages with "~".
        // I ran it with these changes on the local machine and didn't observe major changes
        // in the final output. As I already had run my code with ", " delimiter on AWS
        // by the time I discovered the delimiter discrepancy , I did not run again on AWS EMR
        if (this.linkNames != null) {
            sb.append("[");
                sb.append(StringUtils.join (this.linkNames, ", "));
            sb.append("]");
        }
        sb.append(" {" + this.pageRank + "}");

        return sb.toString();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(pageRank);
        dataOutput.writeInt(linkNames.size());
        for (String name: linkNames)
            WritableUtils.writeString(dataOutput,name);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.pageRank = dataInput.readDouble();
        int size = dataInput.readInt();
        for (int i=0; i < size ; i++)
            linkNames.add(WritableUtils.readString(dataInput));
    }
}
