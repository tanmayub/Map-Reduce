package com.mr.assign1.weather;

import com.mr.assign1.weather.ReadData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by tanmayub on 1/25/17.
 */
public class SequentialVersion {
    private static HashMap<String, List<Double>> hm = new HashMap<>();

	/**
	 * Summary: Entry point for the program
	 * Purpose: Read the file and call sequential execution method. Calculates average, max, min running time
	 */
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        System.out.println("Reading file..");
        //read file
        List<String> listData = ReadData.readInputFile("1912.csv");
        long endTime = System.currentTimeMillis();
        System.out.println(endTime - startTime);

        long min = Long.MAX_VALUE, max = 0, avg = 0;
        for(int i = 0; i < 10; i++) {
            hm = new HashMap<>();
            startTime = System.currentTimeMillis();
            calculateSumofTmax(listData);
            endTime = System.currentTimeMillis();

            long currTime = (endTime - startTime);
            avg += currTime;
            min = currTime < min ? currTime : min;
            max = currTime > max ? currTime : max;
        }
        avg = avg / 10;
        System.out.println("Sequential running times: Avg: " + avg + ", Min: " + min + ", Max: " + max);
        System.out.println("Hashmap size: " + hm.size());
    }

	/**
	 * Summary: Fills up the hashmap whih holds stationId as Key and a list of (running sum, count of items, running average)
	 * Input: list of records
	 * Output: void
	 */
    public static void calculateSumofTmax(List<String> listData) {
        for (String item : listData) {
            String[] array = item.split(",");
			//checks if record has type = TMAX
            if(array[2].toLowerCase().equals("tmax")) {
                List<Double> a2 = new ArrayList<>();
				//checks if key exists in hashmap, if yes updates sum, count and average
                if(hm.containsKey(array[0])) {
                    a2 = hm.get(array[0]);
                    a2.set(0, a2.get(0) + Double.parseDouble(array[3]));
                    a2.set(1, a2.get(1) + 1);
                    a2.set(2, a2.get(0)/a2.get(1));
                    hm.put(array[0], a2);
                    ReadData.fibonacci(17);
                }
                else {//if key is not present in hashmap, adds it
                    a2.add(Double.parseDouble(array[3]));
                    a2.add(1.0);
                    a2.add(a2.get(0)/a2.get(1));
                    hm.put(array[0], a2);
                }
            }
        }
    }
}
