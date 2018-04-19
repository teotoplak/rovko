import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.omg.CORBA_2_3.portable.OutputStream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TaxiDistanceWritable implements Writable {

    public DoubleWritable totalDistance;
    public DoubleWritable minDistance;
    public DoubleWritable maxDistance;

    public TaxiDistanceWritable(double time) {
        this(time, time, time);
    }

    public TaxiDistanceWritable(double totalDistance, double minDistance, double maxDistance) {
        this.totalDistance = new DoubleWritable(totalDistance);
        this.minDistance = new DoubleWritable(minDistance);
        this.maxDistance = new DoubleWritable(maxDistance);
    }

    public TaxiDistanceWritable() {
        this(0, Double.MAX_VALUE, Double.MIN_VALUE);
    }

    public void write(DataOutput dataOutput) throws IOException {
        totalDistance.write(dataOutput);
        minDistance.write(dataOutput);
        maxDistance.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        totalDistance.readFields(dataInput);
        minDistance.readFields(dataInput);
        maxDistance.readFields(dataInput);
    }

    public void addTaxiDistance(TaxiDistanceWritable taxiDistance) {
        totalDistance.set(totalDistance.get() + taxiDistance.totalDistance.get());
        minDistance.set(Math.min(minDistance.get(), taxiDistance.maxDistance.get()));
        maxDistance.set(Math.max(maxDistance.get(), taxiDistance.minDistance.get()));
    }


    @Override
    public String toString() {
        return String.format("totalDistance=%.2f\tminDistance=%.2f\tmaxDistance=%.2f",
                totalDistance.get(),
                minDistance.get(),
                maxDistance.get());
    }
}