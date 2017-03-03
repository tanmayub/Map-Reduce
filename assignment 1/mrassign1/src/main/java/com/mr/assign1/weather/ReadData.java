package com.mr.assign1.weather;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by TanmayPC on 1/22/2017.
 */
public class ReadData {
    /*public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        System.out.println("Reading file..");
        //read file
        List<String> listData = readInputFile("1912.csv");
        long endTime = System.currentTimeMillis();
        System.out.println(endTime - startTime);

        //Sequential
        startTime = System.currentTimeMillis();
        System.out.println("\n\nRunning sequential version");
        com.mr.assign1.weather.SequentialVersion.run(listData);
        endTime = System.currentTimeMillis();
        System.out.println("Running time: " + (endTime - startTime));

        //No lock
        startTime = System.currentTimeMillis();
        System.out.println("\n\nRunning no lock version");
        com.mr.assign1.weather.NoLockVersion.run(listData);
        endTime = System.currentTimeMillis();
        System.out.println("Running time: " + (endTime - startTime));

        //Coarse lock
        startTime = System.currentTimeMillis();
        System.out.println("\n\nRunning coarse lock version");
        com.mr.assign1.weather.CoarseLockVersion.run(listData);
        endTime = System.currentTimeMillis();
        System.out.println("Running time: " + (endTime - startTime));

        //Fine lock
        startTime = System.currentTimeMillis();
        System.out.println("\n\nRunning fine lock version");
        FineLockVersion.run(listData);
        endTime = System.currentTimeMillis();
        System.out.println("Running time: " + (endTime - startTime));

        //Not shared
        startTime = System.currentTimeMillis();
        System.out.println("\n\nRunning not shared version");
        com.mr.assign1.weather.NotSharedVersion.run(listData);
        endTime = System.currentTimeMillis();
        System.out.println("Running time: " + (endTime - startTime));
    }*/

    public static List<String> readInputFile(String fileName) throws IOException{
        List<String> listData = new ArrayList<>();

        //reading file
        File dir = new File(".");
        File fin = new File(dir.getCanonicalPath() + File.separator + "/" + fileName);

        BufferedReader br = new BufferedReader(new FileReader(fin));
        String line;
        while((line = br.readLine()) != null)
            listData.add(line);

        return listData;
    }

    public static void fibonacci(int n) {
        int f = 0, s = 1, ct = 2;
        if(n == 0)
            return;
        if(n == 1)
            return;

        while(ct <= n) {
            int next = f + s;
            f = s;
            s = next;
            ct++;
        }
    }
}