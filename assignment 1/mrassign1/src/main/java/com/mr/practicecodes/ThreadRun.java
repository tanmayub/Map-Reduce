package com.mr.practicecodes;

/**
 * Created by tanmayub on 1/24/17.
 */
class ThreadDemo extends Thread{
    private Thread t;
    private String threadName;

    public ThreadDemo(String name) {
        threadName = name;
        System.out.println("Creating: " + threadName);
    }

    public void run() {
        System.out.println("Running: " + threadName);
        try {
            for(int i = 0; i < 4 ; i++) {
                System.out.println("Thread: " + threadName + ", " + i);
                Thread.sleep(500);
            }
        }
        catch (InterruptedException e) {
            System.out.println("Thread: " + threadName + " interrupted");
        }
        System.out.println("Thread: " + threadName + " exiting");
    }

    public void start() {
        System.out.println("Starting: " + threadName);
        if(t == null) {
            t = new Thread(this, threadName);
            t.start();
        }
    }
}

public class ThreadRun {
    public static void main(String[] args) throws Exception {
        ThreadDemo t1 = new ThreadDemo("Thread1");
        t1.start();

        ThreadDemo t2 = new ThreadDemo("Thread2");
        t2.start();
    }
}
