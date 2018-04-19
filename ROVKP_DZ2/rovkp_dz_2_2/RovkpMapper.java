
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class RovkpMapper extends Mapper<LongWritable, Text, RovkpWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) {

        DEBSRecordParser parser = new DEBSRecordParser();

        if (key.get() > 0) {
            String record = value.toString();
            try {
                parser.parse(record);
                context.write(new RovkpWritable(
                        parser.getMedallion(),
                        parser.getStartLng(),
                        parser.getStartLat(),
                        parser.getEndLng(),
                        parser.getEndLat(),
                        parser.getPassengerCount()
                ), value);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
}
