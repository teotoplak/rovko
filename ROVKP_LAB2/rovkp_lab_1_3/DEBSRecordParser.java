/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

public class DEBSRecordParser {

    private static final double BEGIN_LNG = -74.913585;
    private static final double END_LNG = -73.117785;
    private static final double BEGIN_LAT = 41.474937;
    private static final double END_LAT = 40.1274702;
    private static final double GRID_LENGTH = 0.008983112;
    private static final double GRID_WIDTH = 0.011972;

    private double distance;
    private double startLng;
    private double startLat;
    private double endLng;
    private double endLat;
    private double amountPaid;
    private int startHour;

    public void parse(String record) {
        String[] splitted = record.split(",");
        startLng = Double.parseDouble(splitted[6]);
        startLat = Double.parseDouble(splitted[7]);
        endLng = Double.parseDouble(splitted[8]);
        endLat = Double.parseDouble(splitted[9]);
        distance = Double.parseDouble(splitted[9]);
        amountPaid = Double.parseDouble(splitted[16]);
        startHour = Integer.parseInt(splitted[2].substring(11, 13));
    }

    public boolean insideCellsSpace() {
        return inRange(startLng, BEGIN_LNG, END_LNG) &&
                inRange(startLat, END_LAT, BEGIN_LAT) &&
                inRange(endLng, BEGIN_LNG, END_LNG) &&
                inRange(endLat, END_LAT, BEGIN_LAT);
    }

    private static boolean inRange(double x, double min, double max) {
        return x >= min && x <= max;
    }

    public String getStartingCellNumber() {
        return getCellNumberForLatLng(startLng,startLat);
    }

    public String getEndingCellNumber() {
        return getCellNumberForLatLng(endLng, endLat);
    }

    private String getCellNumberForLatLng(double lng, double lat) {
        int cellId1 =  ((int)((lng - BEGIN_LNG) / GRID_LENGTH)) + 1;
        int cellId2 =   ((int)((BEGIN_LAT - lat) / GRID_WIDTH)) + 1;
        return cellId1 + "." + cellId2;
    }

    public double getAmountPaid() {
        return amountPaid;
    }

    public double getDistance() {
        return distance;
    }

    public double getStartLng() {
        return startLng;
    }

    public double getStartLat() {
        return startLat;
    }

    public double getEndLng() {
        return endLng;
    }

    public double getEndLat() {
        return endLat;
    }

    public int getStartHour() {
        return startHour;
    }
}
