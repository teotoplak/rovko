/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Krešimir Pripužić <kresimir.pripuzic@fer.hr>
 */
public class DEBSRecordParser {

    private String medallion;
    private double distance;

    public void parse(String record) throws Exception {
        String[] splitted = record.split(",");
        medallion = splitted[0];
        distance = Double.parseDouble(splitted[9]);
    }

    public String getMedallion() {
        return medallion;
    }

    public double getDistance() {
        return distance;
    }

}
