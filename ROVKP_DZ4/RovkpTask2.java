import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.NoSuchElementException;

public class RovkpTask2 {

    private static final String INPUT_FILE = "StateNames.csv";

    public static void main(String[] args) {


        SparkConf conf = new SparkConf().setAppName("Rovkp");
        try {
            conf.get("spark.master");
        } catch (NoSuchElementException ex) {
            conf.setMaster("local");
        }

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(INPUT_FILE);

        JavaRDD<USBabyNameRecord> records = lines
                .filter(USBabyNameRecord::isParsable)
                .map(USBabyNameRecord::new)
                .cache();


        mostUnpopularFemaleName(records);
        tenMostPopularMaleNames(records);
        countryWithMostBornBabiesIn1946(records);
        numberOfFemaleBabiesPerYear(records);
        numberOfFemaleBabiesPerYear(records);
        percentageOfNameMaryPerYear(records, numberOfFemaleBabiesPerYear(records));
        totalNumberOfBabies(records);
        numberOfUniqueNames(records);

    }

    private static void mostUnpopularFemaleName(JavaRDD<USBabyNameRecord> records) {

        String result = records
                .filter(USBabyNameRecord::isFemale)
                .mapToPair(record -> new Tuple2<>(record.getName(), record.getCount()))
                .reduceByKey((x,y) -> x + y)
                .min((Comparator<Tuple2<String,Long>> & Serializable) (o1,o2) -> Long.compare(o1._2, o2._2))
                ._1;

        System.out.println(result);
    }

    private static void tenMostPopularMaleNames(JavaRDD<USBabyNameRecord> records) {

        records.filter(USBabyNameRecord::isMale)
                .mapToPair(record -> new Tuple2<>(record.getName(), record.getCount()))
                .reduceByKey((x,y) -> x + y)
                .top(10,(Comparator<Tuple2<String,Long>> & Serializable) (o1,o2) -> Long.compare(o1._2, o2._2))
                .forEach(System.out::println);

    }

    private static void countryWithMostBornBabiesIn1946(JavaRDD<USBabyNameRecord> records) {

        String result = records.filter((record) -> record.getYear().equals(1946))
                .mapToPair(record -> new Tuple2<>(record.getState(), record.getCount()))
                .reduceByKey((x,y) -> x + y)
                .max((Comparator<Tuple2<String,Long>> & Serializable) (o1,o2) -> Long.compare(o1._2, o2._2))
                ._1;

        System.out.println(result);

    }

    private static JavaPairRDD<Integer, Long> numberOfFemaleBabiesPerYear(JavaRDD<USBabyNameRecord> records) {

        JavaPairRDD<Integer, Long> femaleBabiesPerYear = records.filter(USBabyNameRecord::isFemale)
                .mapToPair(record -> new Tuple2<>(record.getYear(), record.getCount()))
                .reduceByKey((x,y) -> x + y)
                .sortByKey();

                femaleBabiesPerYear.collect().forEach(System.out::println);

                return femaleBabiesPerYear;

    }

    private static void percentageOfNameMaryPerYear(JavaRDD<USBabyNameRecord> records,
                                                    JavaPairRDD<Integer, Long> femaleBabiesPerYear) {

        records.filter(USBabyNameRecord::isFemale)
                .filter((record) -> record.getName().equals("Mary"))
                .mapToPair(record -> new Tuple2<>(record.getYear(), record.getCount()))
                .reduceByKey((x,y) -> x + y)
                .join(femaleBabiesPerYear)
                .mapToPair(t -> new Tuple2<>(t._1, 1. * t._2._1 / t._2._2))
                .sortByKey()
                .collect()
                .forEach(System.out::println);

    }

    private static void totalNumberOfBabies(JavaRDD<USBabyNameRecord> records) {

        Long result = records.map(USBabyNameRecord::getCount)
                .reduce((x,y) -> x + y);

        System.out.println(result);

    }

    private static void numberOfUniqueNames(JavaRDD<USBabyNameRecord> records) {

        Long result = records.map(USBabyNameRecord::getName)
                .distinct()
                .count();

        System.out.println(result);

    }



}
