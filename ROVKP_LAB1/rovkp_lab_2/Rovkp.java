import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class Rovkp {

    private static final String OUTPUT_FILE = "ocitanja.bin";
    private static final Integer NUM_OF_READINGS = 100000;
    private static final Integer NUM_OF_SENSORS = 100;
    private static final Random random = ThreadLocalRandom.current();


    public static void main(String[] args) throws IOException, URISyntaxException {



        if (args.length != 1) {
            System.err.println("Expecting one arguments: <hdfs directory>");
            return;
        }
        org.apache.hadoop.fs.Path hdfsFile = new org.apache.hadoop.fs.Path(args[0] + OUTPUT_FILE);

        Configuration configuration = new Configuration();
        org.apache.hadoop.fs.FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(new URI("hdfs://cloudera2:8020"), configuration);

        hdfs.createNewFile(hdfsFile);

        SequenceFile.Writer writer = SequenceFile.createWriter(configuration,
                SequenceFile.Writer.file(hdfsFile),
                SequenceFile.Writer.keyClass(IntWritable.class),
                SequenceFile.Writer.valueClass(FloatWritable.class));


        IntWritable key = new IntWritable();
        FloatWritable value = new FloatWritable();
        for (int reading = 0; reading < NUM_OF_READINGS; reading++) {
            Integer randomKey = random.nextInt(NUM_OF_SENSORS);
            Float randomValue = random.nextFloat() * 100;
            key.set(randomKey);
            value.set(randomValue);
            writer.append(key,value);
        }
        writer.close();

        System.out.println("Finished writing to " + hdfsFile.toUri().toString());
        System.out.println("Reading..");

        SequenceFile.Reader reader = new SequenceFile.Reader(configuration,
                SequenceFile.Reader.file(hdfsFile));


        float[] sum = new float[NUM_OF_SENSORS];
        int[] numOfReadings = new int[NUM_OF_SENSORS];
        for (int index = 0; index < NUM_OF_SENSORS; index++) {
            numOfReadings[index] = 0;
            sum[index] = 0f;
        }
        while (reader.next(key, value)) {
            sum[key.get()] += value.get();
            numOfReadings[key.get()]++;
        }
        for (int index = 0; index < NUM_OF_SENSORS; index ++) {
            float avg = numOfReadings[index] == 0 ? 0 : sum[index] / numOfReadings[index];
            System.out.printf("Senzor %d: %.2f [readings: %d]%n", index + 1, avg, numOfReadings[index]);
        }


    }

}
