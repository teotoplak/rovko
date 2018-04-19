import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RovkpMapper extends Mapper<LongWritable, Text, Text, RovkpWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) {

        DEBSRecordParser parser = new DEBSRecordParser();

        String record = value.toString();
        try {
            parser.parse(record);
            if (true) {
                context.write(new Text(parser.getStartingCellNumber()),
                        new RovkpWritable(parser.getAmountPaid(),parser.getStartHour())
                        );
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
