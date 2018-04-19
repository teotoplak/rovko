import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RovkpWritable implements WritableComparable<RovkpWritable> {

    public Text badge;
    public DoubleWritable startLng;
    public DoubleWritable startLat;
    public DoubleWritable endLng;
    public DoubleWritable endLat;
    public IntWritable passengerCount;

    public RovkpWritable() {
        badge = new Text();
        startLng = new DoubleWritable();
        startLat = new DoubleWritable();
        endLng = new DoubleWritable();
        endLat = new DoubleWritable();
        passengerCount = new IntWritable();
    }

    public RovkpWritable(String badge, Double startLng, Double startLat, Double endLng, Double endLat, Integer passengerCount) {
        this.badge = new Text(badge);
        this.startLng = new DoubleWritable(startLng);
        this.startLat = new DoubleWritable(startLat);
        this.endLng = new DoubleWritable(endLng);
        this.endLat = new DoubleWritable(endLat);
        this.passengerCount = new IntWritable(passengerCount);
    }

    public void write(DataOutput dataOutput) throws IOException {
        badge.write(dataOutput);
        startLng.write(dataOutput);
        startLat.write(dataOutput);
        endLng.write(dataOutput);
        endLat.write(dataOutput);
        passengerCount.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        badge.readFields(dataInput);
        startLng.readFields(dataInput);
        startLat.readFields(dataInput);
        endLng.readFields(dataInput);
        endLat.readFields(dataInput);
        passengerCount.readFields(dataInput);;
    }

    public Double getStartLng() {
        return startLng.get();
    }

    public Double getStartLat() {
        return startLat.get();
    }

    public Double getEndLng() {
        return endLng.get();
    }

    public Double getEndLat() {
        return endLat.get();
    }

    public Integer getPassengerCount() {
        return passengerCount.get();
    }

    public Text getBadge() {
        return badge;
    }

    @Override
    public int hashCode() {
        return badge.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RovkpWritable) {
            RovkpWritable writable = (RovkpWritable) obj;
            return this.getBadge().equals(writable.getBadge());
        }
        return false;
    }

    public int compareTo(RovkpWritable o) {
        return this.startLng.compareTo(o.startLng);
    }
}