import java.io.Serializable;
import java.util.Comparator;

public class SensorScopeReading implements Serializable {
    private String fullRecord;
    private Long timestamp;

    public SensorScopeReading(long timestamp, String fullRecord) {
        this.timestamp = timestamp;
        this.fullRecord = fullRecord;
    }

    public String getFullRecord() {
        return fullRecord;
    }

    public String getFullRecordInCSVFormat() {
        return fullRecord.replaceAll("\\s+", ",");
    }

    public Long getTimestamp() {
        return timestamp;
    }

}
