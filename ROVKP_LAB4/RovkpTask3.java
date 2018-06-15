import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.NoSuchElementException;

public class RovkpTask3 {

    private static final Integer SERVER_PORT = 10002;
    private static final String OUTPUT_FILE = "scope-reading-max";

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("Rovkp");
        try {
            conf.get("spark.master");
        } catch (NoSuchElementException ex) {
            conf.setMaster("local[2]");
        }
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(3));
        JavaDStream<String> lines = jssc.socketTextStream("localhost", SERVER_PORT);

        lines.filter(PollutionDataFullRecord::isParsable)
                .map(PollutionDataFullRecord::new)
                .mapToPair(reading -> new Tuple2<>(reading.getLatitude() + "," + reading.getLongitude(), reading.getOzone()))
                .reduceByKeyAndWindow(Math::max, Durations.seconds(45), Durations.seconds(15))
                .dstream()
                .saveAsTextFiles(OUTPUT_FILE, "txt");

        jssc.start();
        jssc.awaitTermination();

    }
}
