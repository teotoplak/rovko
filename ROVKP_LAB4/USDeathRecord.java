import enums.Autopsy;
import enums.MartialStatus;
import enums.Sex;

public class USDeathRecord {

    private int monthOfDeath;
    private Sex sex;
    private int age;
    private MartialStatus martialStatus;
    private int dayOfWeekOfDeath;
    private String mannerOfDeath;
    private Autopsy autopsy;

    static USDeathRecord create(String line) {
        try {
            return new USDeathRecord(line);
        } catch (Throwable t) {
            return null;
        }
    }

    public USDeathRecord(String line) {
        String[] split = line.split(",");

        monthOfDeath = Integer.parseInt(split[5]);
        sex = Sex.valueOf(split[6]);
        age = Integer.parseInt(split[8]);
        martialStatus = MartialStatus.valueOf(split[15]);
        dayOfWeekOfDeath = Integer.parseInt(split[16]);
        mannerOfDeath = split[19];
        autopsy = Autopsy.valueOf(split[21]);

    }

    public static boolean isParsable(String line) {
        try {
            new USDeathRecord(line);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }


    public int getMonthOfDeath() {
        return monthOfDeath;
    }

    public Sex getSex() {
        return sex;
    }

    public int getAge() {
        return age;
    }

    public MartialStatus getMartialStatus() {
        return martialStatus;
    }

    public int getDayOfWeekOfDeath() {
        return dayOfWeekOfDeath;
    }

    public String getMannerOfDeath() {
        return mannerOfDeath;
    }

    public Autopsy getAutopsy() {
        return autopsy;
    }
}
