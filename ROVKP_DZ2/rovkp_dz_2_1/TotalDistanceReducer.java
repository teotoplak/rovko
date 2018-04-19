
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalDistanceReducer
        extends Reducer<Text, TaxiDistanceWritable, Text, TaxiDistanceWritable> {


    @Override
    public void reduce(Text key, Iterable<TaxiDistanceWritable> values, Context context) throws IOException, InterruptedException {
        TaxiDistanceWritable taxiDistance = new TaxiDistanceWritable();
        for (TaxiDistanceWritable value : values) {
            taxiDistance.addTaxiDistance(value);
        }
        context.write(key, taxiDistance);

    }
}
