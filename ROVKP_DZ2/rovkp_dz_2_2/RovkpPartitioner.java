import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class RovkpPartitioner extends Partitioner<RovkpWritable, Text> {

    private static final double LNG_MIN = -74.0;
    private static final double LNG_MAX = -73.95;
    private static final double LAT_MIN = 40.75;
    private static final double LAT_MAX = 40.8;

    @Override
    public int getPartition(RovkpWritable rovkpWritable, Text text, int numberOfPartitions) {

        int key;

        key = inRange(rovkpWritable.getStartLng(), LNG_MIN, LNG_MAX)
                && inRange(rovkpWritable.getStartLat(), LAT_MIN, LAT_MAX)
                && inRange(rovkpWritable.getEndLng(), LNG_MIN, LNG_MAX)
                && inRange(rovkpWritable.getEndLat(), LAT_MIN, LAT_MAX) ? 0 : 3;

        switch (rovkpWritable.getPassengerCount()) {
            case 1:
                key += 0;
                break;
            case 2:
            case 3:
                key += 1;
                break;
            default:
                key += 2;
        }

        return key;
    }


    private static boolean inRange(double x, double min, double max) {
        return x >= min && x <= max;
    }
}