/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

public class DEBSRecordParser {

    private String medallion;
    private double distance;
    private double startLng;
    private double startLat;
    private double endLng;
    private double endLat;
    private int passengerCount;

    public void parse(String record) {
        String[] splitted = record.split(",");
        medallion = splitted[0];
        startLng = Double.parseDouble(splitted[10]);
        startLat = Double.parseDouble(splitted[11]);
        endLng = Double.parseDouble(splitted[12]);
        endLat = Double.parseDouble(splitted[13]);
        distance = Double.parseDouble(splitted[9]);
        passengerCount = Integer.parseInt(splitted[7]);
    }

    public String getMedallion() {
        return medallion;
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

    public int getPassengerCount() {
        return passengerCount;
    }
}
