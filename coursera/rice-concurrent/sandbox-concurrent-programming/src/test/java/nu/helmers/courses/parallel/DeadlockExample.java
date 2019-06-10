package nu.helmers.courses.parallel;

import java.util.stream.IntStream;

/** */
public class DeadlockExample {

    private static Object lock1 = new Object();
    private static Object lock2 = new Object();

    public static void main(String[] args) {
        Thread thread1 = new ThreadA();
//        Thread thread2 = new ThreadB();

        thread1.start();
//        thread2.start();
    }
    private static class ThreadA extends Thread {

        @Override
        public void run() {
            System.out.println(Thread.currentThread().getName() + " waiting for lock1");
            synchronized (lock1) {

            }

            IntStream.range(0, 10).parallel().toArray();


        }
    }
}
