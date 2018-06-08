import java.io.Serializable;

public class SensorScopeReading2 implements Serializable {

    private Long stationID;
    private Double solarPanelCurrent;

    public SensorScopeReading2(String line) {
        String[] split = line.split(",");
        this.stationID = Long.parseLong(split[0]);
        this.solarPanelCurrent = Double.parseDouble(split[16]);
    }

    public Long getStationID() {
        return stationID;
    }

    public Double getSolarPanelCurrent() {
        return solarPanelCurrent;
    }
}
