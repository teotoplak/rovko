
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TotalDistanceMapper extends Mapper<LongWritable, Text, Text, TaxiDistanceWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        DEBSRecordParser parser = new DEBSRecordParser();

        //skip the first line
        if (key.get() > 0) {
            String record = value.toString();
            try {
                parser.parse(record);
                context.write(new Text(parser.getMedallion()), new TaxiDistanceWritable(parser.getDistance()));
            } catch (Exception ex) {
                //System.out.println("Cannot parse: " + record + "due to the " + ex);
            }
        }
    }
}
