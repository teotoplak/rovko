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

    private static final String OUTPUT_FILE = "pollutionData-all.csv";

    public static void main(String[] args) throws IOException {

        PrintWriter pw = new PrintWriter(Files.newOutputStream(Paths.get(OUTPUT_FILE)));

        System.out.println("This could take some time, please wait ...");

        Files.list(Paths.get("pollution-data"))
                .flatMap(ThrowingFunction.wrap(Files::lines))
                .filter(PollutionDataFullRecord::isParsable)
                .map(PollutionDataFullRecord::new)
                .sorted(Comparator.comparing(PollutionDataFullRecord::getTimestamp))
                .map(data -> data.getFullDataCSVLine())
                .distinct()
                .forEach(pw::println);

        System.out.println("Finished!");

        pw.close();

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

