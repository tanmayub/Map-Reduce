package com.mr.assign1.weather;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by tanmayub on 1/24/17.
 */
 
/**
 * Summary: This class manages threaded execution for the main class
 * Variables: Thread t, a list which holds all the threads(used at the last to consolidate all threads), thread name,
 *  		   Input list(each thread has its own chunk of input data), hashmap which will be initialized in the constructor
 */
class ThreadedExecutionWithFine extends Thread{
    private Thread t;
    private String threadName;
    private List<Thread> threadList = new ArrayList<>();;
    private List<String> listInput;
    private ConcurrentHashMap<String, List<Double>> hm;

	/**
	 * Constructor: assigns name, input chunk, instance of common hashmap
	 */
    public ThreadedExecutionWithFine(String name, List<String> input, ConcurrentHashMap<String, List<Double>> map) {
        threadName = name;
        listInput = input;
        hm = map;
    }

	/**
	 * run: this is overridden method from Thread class. Every thred calls this method.
	 */
    public void run() {
        //call calculateSumofTmax at some point
        calculateSumofTmax(listInput);
        //System.out.println("Thread: " + threadName + " exiting");
        //System.out.println("Hashmap size: " + hm.size());
    }

	/**
	 * start: This method will be called when a thread is to be spawned. Thi method calls thread.start method for each thread
	 */
    public void start() {
        if(t == null) {
            t = new Thread(this, threadName);
            t.start();
            threadList.add(t);
        }
    }

	/**
	 * Summary: Fills up the hashmap whih holds stationId as Key and a list of (running sum, count of items, running average)
	 * Input: list of records
	 * Output: void
	 */
    public void calculateSumofTmax(List<String> listData) {
        for (String item : listData) {
            String[] array = item.split(",");
			//checks if record has type = TMAX
            if(array[2].toLowerCase().equals("tmax")) {
                List<Double> a2 = new ArrayList<>();
				//checks if key exists in hashmap, if yes updates sum, count and average
                if(hm.containsKey(array[0])) {
					//put a lock on the record while updating it
                    synchronized (hm.get(array[0])) {
                        a2 = hm.get(array[0]);
                        a2.set(0, a2.get(0) + Double.parseDouble(array[3]));
                        a2.set(1, a2.get(1) + 1);
                        a2.set(2, a2.get(0)/a2.get(1));
                        hm.put(array[0], a2);
                        ReadData.fibonacci(17);
                    }
                }
                else {//if key is not present in hashmap, adds it
                    hm.put(array[0], new ArrayList<Double>());
					//put a lock on record while inserting it
                    synchronized (hm.get(array[0])) {
                        a2 = hm.get(array[0]);
                        a2.add(Double.parseDouble(array[3]));
                        a2.add(1.0);
                        a2.add(a2.get(0)/a2.get(1));
                        hm.put(array[0], a2);
                    }
                }
            }
        }
    }

	/**
	 * Summary: This method consolidates all the threads so that calling method will wait for all threads to finish before 
	 *          returning to the main method.
	 */
    public void waitForCompletionFine() {
        try {
            for (int i = 0; i < threadList.size(); i++)
                threadList.get(i).join();
        }
        catch (InterruptedException ex) {
            System.out.println("Interrupted");
        }
    }
}

public class FineLockVersion {
	/**
	 * Summary: Entry point for the program
	 * Purpose: Read the file and call no lock run method. Calculates average, max, min running time
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
            startTime = System.currentTimeMillis();
            run(listData);
            endTime = System.currentTimeMillis();

            long currTime = (endTime - startTime);
            avg += currTime;
            min = currTime < min ? currTime : min;
            max = currTime > max ? currTime : max;
        }
        avg = avg / 10;
        System.out.println("FineLock running times: Avg: " + avg + ", Min: " + min + ", Max: " + max);
    }

	/**
	 * Summary: Spawns multiple threads, distributes the input among them and starts thread execution
	 * Input: list of records
	 * Output: void
	 */
    public static void run(List<String> listData) {
        int quarterSize = listData.size() / 4;
		//common hashmap-- since it is concurrent it ensures record(hashmap bucket) level locks and very high concurrency
        ConcurrentHashMap<String, List<Double>> map = new ConcurrentHashMap<>();

        //System.out.println("Creating threads");
        ThreadedExecutionWithFine t1 = new ThreadedExecutionWithFine("Thread1", listData.subList(0, quarterSize), map);
        t1.start();
        ThreadedExecutionWithFine t2 = new ThreadedExecutionWithFine("Thread2", listData.subList(quarterSize, 2 * quarterSize), map);
        t2.start();
        ThreadedExecutionWithFine t3 = new ThreadedExecutionWithFine("Thread3", listData.subList(2 * quarterSize, 3 * quarterSize), map);
        t3.start();
        ThreadedExecutionWithFine t4 = new ThreadedExecutionWithFine("Thread4", listData.subList(3 * quarterSize, listData.size()), map);
        t4.start();

        t1.waitForCompletionFine();
        t2.waitForCompletionFine();
        t3.waitForCompletionFine();
        t4.waitForCompletionFine();
    }
}
