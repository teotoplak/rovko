import enums.Autopsy;
import enums.MartialStatus;
import enums.Sex;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.NoSuchElementException;

public class RovkpTask2 {

    private static final String INPUT_FILE = "DeathRecords.csv";
    private static final String INPUT_FILE_DEMO = "DeathRecordsDemo.csv";

    public static void main(String[] args) {


        SparkConf conf = new SparkConf().setAppName("Rovkp");
        try {
            conf.get("spark.master");
        } catch (NoSuchElementException ex) {
            conf.setMaster("local");
        }

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(INPUT_FILE);


        JavaRDD<USDeathRecord> records = lines
                .filter(USDeathRecord::isParsable)
                .map(USDeathRecord::new)
                .cache();


        //amountOfFemaleDeathOlderThen40(records);
        //monthOfYearWithMostMaleDeathsYoungerThen50(records);
        //numberOfFemaleAutopsies(records);
        //deathsByDayOfWeekForFemaleBetweenAge50And65(records);
        //deathPercentageByDayOfWeekForMarriedFemaleBetweenAge50And65(records, deathsByDayOfWeekForFemaleBetweenAge50And65(records));
        //numberOfAccidentMaleDeaths(records);
        //numberOfDifferentDeathAges(records);

        testy(records);


    }

    private static void amountOfFemaleDeathOlderThen40(JavaRDD<USDeathRecord> records) {

        Long result = records
                .filter(record -> record.getSex() == Sex.F && record.getAge() > 40)
                .count();

        System.out.println(result);
    }

    private static void monthOfYearWithMostMaleDeathsYoungerThen50(JavaRDD<USDeathRecord> records) {

        Integer result = records
                .filter(record -> record.getSex() == Sex.M && record.getAge() < 50)
                .mapToPair(record -> new Tuple2<>(record.getMonthOfDeath(), 1L))
                .reduceByKey((x, y) -> x + y)
                .max((Comparator<Tuple2<Integer,Long>> & Serializable) (o1,o2) -> Long.compare(o1._2, o2._2))
                ._1;

        System.out.println(result);
    }

    private static void numberOfFemaleAutopsies(JavaRDD<USDeathRecord> records) {

        Long result = records
                .filter(record -> record.getSex() == Sex.F && record.getAutopsy() == Autopsy.Y)
                .count();

        System.out.println(result);
    }

    private static JavaPairRDD<Integer, Long> deathsByDayOfWeekForFemaleBetweenAge50And65(JavaRDD<USDeathRecord> records) {

        JavaPairRDD<Integer, Long> result = records.filter(record -> record.getSex() == Sex.F
                        && record.getAge() > 50
                        && record.getAge() < 65)
                .mapToPair(record -> new Tuple2<>(record.getDayOfWeekOfDeath(), 1L))
                .reduceByKey((x, y) -> x + y)
                .sortByKey();

        result.collect().forEach(System.out::println);

        return result;

    }

    private static void testy(JavaRDD<USDeathRecord> records) {

        records.filter(record -> record.getSex() == Sex.F
                        && record.getAge() > 50
                        && record.getAge() < 65
                        && record.getDayOfWeekOfDeath() == 9)
                .mapToPair(record -> new Tuple2<>(record.getDayOfWeekOfDeath(), record.getMonthOfDeath()))
                .collect()
                .forEach(System.out::println);


    }


    private static void deathPercentageByDayOfWeekForMarriedFemaleBetweenAge50And65(JavaRDD<USDeathRecord> records,
                                                                                    JavaPairRDD<Integer, Long> totalStream) {

        records.filter(record -> record.getSex() == Sex.F
                && record.getAge() > 50
                && record.getAge() < 65
                && record.getMartialStatus() == MartialStatus.M)
                .mapToPair(record -> new Tuple2<>(record.getDayOfWeekOfDeath(), 1L))
                .reduceByKey((x, y) -> x + y)
                .join(totalStream)
                .mapToPair(t -> new Tuple2<>(t._1, 1. * t._2._1 / t._2._2))
                .sortByKey()
                .collect()
                .forEach(System.out::println);

    }

    private static void numberOfAccidentMaleDeaths(JavaRDD<USDeathRecord> records) {

        Long result = records
                .filter(record -> record.getSex() == Sex.M && record.getMannerOfDeath().equals("1"))
                .count();

        System.out.println(result);

    }

    private static void numberOfDifferentDeathAges(JavaRDD<USDeathRecord> records) {

        Long result = records
                .map(USDeathRecord::getAge)
                .distinct()
                .count();

        System.out.println(result);

    }


}
