import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RovkpTask1 {

    private static final String OUTPUT_FILE = "senesorscope-monitor-all.csv";

    public static void main(String[] args) throws IOException {

        PrintWriter pw = new PrintWriter(Files.newOutputStream(Paths.get(OUTPUT_FILE)));

        System.out.println("This could take some time, please wait ...");

        Files.list(Paths.get("data"))
                .flatMap(ThrowingFunction.wrap(Files::lines))
                .filter(SensorScopeFullRecord::isParsable)
                .map(line -> new SensorScopeReading(Long.parseLong(line.split("\\s+")[7]),line))
                .sorted(Comparator.comparing(SensorScopeReading::getTimestamp))
                .map(SensorScopeReading::getFullRecordInCSVFormat)
                .forEach(pw::println);

        System.out.println("Finished!");

    }

    @FunctionalInterface
    public interface ThrowingFunction<T,R> extends Function<T,R> {

        @Override
        public default R apply(T t) {
            try {
                return throwingApply(t);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public static<T,R> Function<T,R> wrap(ThrowingFunction<T,R> f) {
            return f;
        }

        R throwingApply(T t) throws Exception;
    }


}

