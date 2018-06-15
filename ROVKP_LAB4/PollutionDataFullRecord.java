public class PollutionDataFullRecord {

    private Integer ozone;
    private Integer particullate_matter;
    private Integer carbon_monoxide;
    private Integer sulfure_dioxide;
    private Integer nitrogen_dioxide;
    private Double longitude;
    private Double latitude;
    private String timestamp;
    private String fullDataCSVLine;

    public PollutionDataFullRecord(String csvLine) {
        String[] split = csvLine.split(",");
        this.ozone = Integer.parseInt(split[0]);
        this.particullate_matter = Integer.parseInt(split[1]);
        this.carbon_monoxide = Integer.parseInt(split[2]);
        this.sulfure_dioxide = Integer.parseInt(split[3]);
        this.nitrogen_dioxide = Integer.parseInt(split[4]);
        this.longitude = Double.parseDouble(split[5]);
        this.latitude = Double.parseDouble(split[6]);
        this.timestamp = split[7];
        this.fullDataCSVLine = csvLine;
    }

    public static boolean isParsable(String line) {
        try {
            new PollutionDataFullRecord(line);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    public Integer getOzone() {
        return ozone;
    }

    public Integer getParticullate_matter() {
        return particullate_matter;
    }

    public Integer getCarbon_monoxide() {
        return carbon_monoxide;
    }

    public Integer getSulfure_dioxide() {
        return sulfure_dioxide;
    }

    public Integer getNitrogen_dioxide() {
        return nitrogen_dioxide;
    }

    public Double getLongitude() {
        return longitude;
    }

    public Double getLatitude() {
        return latitude;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getFullDataCSVLine() {
        return fullDataCSVLine;
    }
}
