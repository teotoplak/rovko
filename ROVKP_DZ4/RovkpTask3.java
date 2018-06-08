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
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        JavaDStream<String> lines = jssc.socketTextStream("localhost", SERVER_PORT);

        lines.filter(SensorScopeFullRecord::isParsable)
                .map(SensorScopeReading2::new)
                .mapToPair(reading -> new Tuple2<>(reading.getStationID(), reading.getSolarPanelCurrent()))
                .reduceByKeyAndWindow(Math::max, Durations.seconds(60), Durations.seconds(10))
                .dstream()
                .saveAsTextFiles(OUTPUT_FILE, "txt");

        jssc.start();
        jssc.awaitTermination();

    }
}
