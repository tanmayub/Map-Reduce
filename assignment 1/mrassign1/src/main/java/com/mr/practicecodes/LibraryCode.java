package com.mr.practicecodes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by tanmayub on 1/24/17.
 */
public class LibraryCode extends Thread{
    public Thread t;
    public List<Thread> threadList = new ArrayList<>();
    public String threadName;
    public List<String> listInput;
    public HashMap<String, List<Integer>> hm;

    public LibraryCode() {

    }

    public LibraryCode(String name, List<String> input, HashMap<String, List<Integer>> map) {
        threadName = name;
        listInput = input;
        hm = map;
    }

    /*public void run() {
        //call calculateSumofTmax at some point
        calculateSumofTmax(listInput);
        System.out.println("Thread: " + threadName + " exiting");
        System.out.println("Hashmap size: " + hm.size());
    }*/

    public void start() {
        if(t == null) {
            t = new Thread(this, threadName);
            t.start();
            threadList.add(t);
        }
    }

    public void calculateSumofTmax(List<String> listData) {
        for (String item : listData) {
            String[] array = item.split(",");
            if(array[2].toLowerCase().equals("tmax")) {
                List<Integer> a2 = new ArrayList<>();
                if(hm.containsKey(array[0])) {
                    a2 = hm.get(array[0]);
                    a2.add(a2.get(0) + Integer.parseInt(array[3]));
                    a2.add(a2.get(1) + 1);
                    hm.put(array[0], a2);
                }
                else {
                    a2.add(Integer.parseInt(array[3]));
                    a2.add(1);
                    hm.put(array[0], a2);
                }
            }
        }
    }

    public void waitForThreadExecutionToComplete() {
        try {
            for (int i = 0; i < threadList.size(); i++)
                threadList.get(i).join();
        }
        catch (InterruptedException ex) {
            System.out.println("Interrupted");
        }
    }
}
