package nu.helmers.courses.parallel;

import edu.rice.pcdp.PCDP;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

/**
 * This class illustrates that ArrayList is not thread-safe, and provides some examples of how to work with lists in a
 * multi-threaded environment (by synchronizing access to ArrayList, or using a thread-safe alternative like
 * {@link java.util.concurrent.CopyOnWriteArrayList}).
 *
 * <p/>
 *
 * Notice that the thread-unsafety of ArrayList can result in two possible test outcomes:
 * <ul>
 *     <li>The assertion that less integers were added than expected holds, and the test succeeds.</li>
 *     <li>Concurrent modification of the ArrayList results in an exception (like ArrayIndexOutOfBoundsException) and
 *     the test fails.</li>
 * </ul>
 * In both cases, the test illustrates that ArrayList is not suited for concurrent modification...
 */
public class ArrayListTest {

    /** This test shows that ArrayList is not thread safe. */
    @Test
    public void arrayListIsNotThreadSafe_Thread() throws InterruptedException {
        ArrayList<Integer> list = new ArrayList<>();

        // Create two threads, and start() and join() them manually.
        Thread t1 = new Thread(new AddToList(list, 0, 500_000));
        Thread t2 = new Thread(new AddToList(list, 500_000, 1_000_000));

        t1.start(); // Do NOT call t1.run() - that will not actually spawn a new thread!
        t2.start();

        t1.join();
        t2.join();

        System.out.printf("Filling ArrayList in parallel using Thread.start(): size %d instead of %d", list.size(), 1_000_000);
        assertThat(list.size(), lessThan(1_000_000));
    }

    /** This test shows that ArrayList is not thread safe (using the ForkJoin framework. */
    @Test
    public void arrayListIsNotThreadSafe_ForkJoin() {
        ArrayList<Integer> list = new ArrayList<>();

        // Create two ForkJoinTasks, submit them to the execution pool, and join() them.
        ForkJoinTask<Void> t1 = new AddToListTask(list, 0, 500_000);
        ForkJoinTask<Void> t2 = new AddToListTask(list, 500_000, 1_000_000);

        ForkJoinPool.commonPool().submit(t1);
        ForkJoinPool.commonPool().submit(t2);

        t1.join();
        t2.join();

        System.out.printf("Filling ArrayList in parallel using Fork/Join framework: size %d instead of %d", list.size(), 1_000_000);
        assertThat(list.size(), lessThan(1_000_000));
    }

    /** This test shows that ArrayList is not thread safe (using the FINISH/ASYNC paradigm from PCDP the library). */
    @Test
    public void arrayListIsNotThreadSafe_PCDP() {
        ArrayList<Integer> list = new ArrayList<>();

        // Use the FINISH/ASYNC paradigm from the PCDP-library; by far the most concise way of expressing parallelism.
        PCDP.finish(() -> {
            PCDP.async(new AddToList(list, 0, 500_000));
            PCDP.async(new AddToList(list, 500_000, 1_000_000));
        });

        System.out.printf("Filling ArrayList in parallel using PCDP-library: size %d instead of %d", list.size(), 1_000_000);
        assertThat(list.size(), lessThan(1_000_000));
    }

    /** This test shows how to use ArrayList in a thread-safe way (by synchronizing access to it). */
    @Test
    public void arrayListIsThreadSafe_Synchronized() {
        ArrayList<Integer> list = new ArrayList<>();

        // Use a runnable that synchronizes access to the array list it's manipulating.
        PCDP.finish(() -> {
            PCDP.async(new AddToListSynchronized(list, 0, 500_000));
            PCDP.async(new AddToListSynchronized(list, 500_000, 1_000_000));
        });

        assertThat(list.size(), is(1_000_000));
    }

    /**
     * CopyOnWriteArrayList is a thread-safe alternative for ArrayList.
     * <p/>
     * This class is EXTREMELY INEFFICIENT because all mutative operations are implemented by making a fresh copy of the
     * underlying array! Use with caution!
     */
    @Test
    public void copyOnWriteArrayListIsThreadSafe() {
        CopyOnWriteArrayList<Integer> list = new CopyOnWriteArrayList<>();

        PCDP.finish(() -> {
            PCDP.async(new AddToList(list, 0, 50_000));
            PCDP.async(new AddToList(list, 50_000, 100_000));
        });

        assertThat(list.size(), is(100_000));
    }

    static class AddToList implements Runnable {
        protected List<Integer> list;
        protected int from;
        protected int to;

        public AddToList(List<Integer> list, int from, int to) {
            this.list = list;
            this.from = from;
            this.to = to;
        }

        public void run() {
            IntStream.range(from, to).forEach(i -> list.add(i));
        }

    }

    static class AddToListSynchronized extends AddToList {
        public AddToListSynchronized(List<Integer> list, int from, int to) {
            super(list, from, to);
        }

        @Override
        public void run() {
            IntStream.range(from, to).forEach(i -> {
                synchronized (list) {
                    list.add(i);
                }
            });
        }
    }

    static class AddToListTask extends RecursiveAction {
        protected List<Integer> list;
        protected int from;
        protected int to;

        public AddToListTask(List<Integer> list, int from, int to) {
            this.list = list;
            this.from = from;
            this.to = to;
        }

        @Override
        protected void compute() {
            IntStream.range(from, to).forEach(i -> list.add(i));
        }
    }
}