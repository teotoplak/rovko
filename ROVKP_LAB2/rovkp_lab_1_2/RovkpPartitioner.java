import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class RovkpPartitioner extends Partitioner<Text, RovkpWritable> {

    @Override
    public int getPartition(Text key, RovkpWritable writable, int numberOfPartitions) {
        return writable.getStartHour();
    }

}