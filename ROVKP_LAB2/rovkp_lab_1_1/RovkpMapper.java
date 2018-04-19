import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RovkpMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) {

        DEBSRecordParser parser = new DEBSRecordParser();

        String record = value.toString();
        try {
            parser.parse(record);
            if (parser.insideCellsSpace() && parser.getAmountPaid() > 0) {
                context.write(NullWritable.get(),value);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
