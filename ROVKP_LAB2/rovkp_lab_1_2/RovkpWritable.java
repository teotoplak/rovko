import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RovkpWritable implements Writable {

    IntWritable numOfRides;
    IntWritable startHour;
    DoubleWritable totalPrice;


    public RovkpWritable() {
        numOfRides = new IntWritable(0);
        totalPrice = new DoubleWritable(0.0);
        startHour = new IntWritable(-1);
    }

    public RovkpWritable(Double totalPrice, Integer startHour) {
        this.numOfRides = new IntWritable(1);
        this.totalPrice = new DoubleWritable(totalPrice);
        this.startHour = new IntWritable(startHour);
    }

    public void write(DataOutput dataOutput) throws IOException {
        numOfRides.write(dataOutput);
        totalPrice.write(dataOutput);
        startHour.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        numOfRides.readFields(dataInput);
        totalPrice.readFields(dataInput);
        startHour.readFields(dataInput);
    }

    public Integer getNumOfRides() {
        return numOfRides.get();
    }

    public Double getTotalPrice() {
        return totalPrice.get();
    }

    public Integer getStartHour() {
        return startHour.get();
    }

    @Override
    public String toString() {
        return numOfRides.get() + " " + startHour.get();
    }
}