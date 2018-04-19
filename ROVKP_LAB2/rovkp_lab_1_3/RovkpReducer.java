
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RovkpReducer
        extends Reducer<Text, RovkpWritable, NullWritable, Text> {

    Integer starHour;

    String cellMaxNumOfRides;
    Integer maxNumOfRides;

    String cellMaxTotalPrice;
    Double maxTotalPrice;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        maxNumOfRides = Integer.MIN_VALUE;
        maxTotalPrice = Double.MIN_VALUE;
    }

    @Override
    public void reduce(Text key, Iterable<RovkpWritable> values, Context context) throws IOException, InterruptedException {
        Integer numOfRides = 0;
        Double totalPrice = Double.MIN_VALUE;
        for (RovkpWritable writable : values) {
            starHour = writable.getStartHour();
            numOfRides ++;
            totalPrice += writable.getTotalPrice();
        }
        if (maxNumOfRides < numOfRides) {
            maxNumOfRides = numOfRides;
            cellMaxNumOfRides = key.toString();
        }
        if (maxTotalPrice < totalPrice) {
            maxTotalPrice = totalPrice;
            cellMaxTotalPrice = key.toString();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(),new Text(starHour + ""));
        context.write(NullWritable.get(), new Text(cellMaxNumOfRides + " " + maxNumOfRides));
        context.write(NullWritable.get(), new Text(cellMaxTotalPrice + " " + maxTotalPrice));
    }
}
