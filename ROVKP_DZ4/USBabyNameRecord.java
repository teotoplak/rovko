import java.io.Serializable;

public class USBabyNameRecord implements Serializable {

    private static final String MALE_GENDER_KEYWORD = "M";
    private static final String FEMALE_GENDER_KEYWORD = "F";

    private Long id;
    private String name;
    private Integer year;
    private String gender;
    private String state;
    private Long count;

    public USBabyNameRecord(String line) {
        String[] split = line.split(",");
        this.id = Long.parseLong(split[0]);
        this.name = split[1];
        this.year = Integer.parseInt(split[2]);
        this.gender = split[3];
        this.state = split[4];
        this.count = Long.parseLong(split[5]);
    }

    public static boolean isParsable(String line) {
        try {
            new USBabyNameRecord(line);
        } catch (Exception ex) {
            return false;
        }
        return true;
    }

    public boolean isMale() {
        return gender.equals(MALE_GENDER_KEYWORD);
    }

    public boolean isFemale() {
        return gender.equals(FEMALE_GENDER_KEYWORD);
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Integer getYear() {
        return year;
    }

    public String getGender() {
        return gender;
    }

    public String getState() {
        return state;
    }

    public Long getCount() {
        return count;
    }
}
