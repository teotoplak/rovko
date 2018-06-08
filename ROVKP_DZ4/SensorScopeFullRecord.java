public class SensorScopeFullRecord {

    public static boolean isParsable(String line) {
        // todo real implementation with parsing every value
        String[] split = line.split("\\s+");
        return split.length == 19;
    }

}
