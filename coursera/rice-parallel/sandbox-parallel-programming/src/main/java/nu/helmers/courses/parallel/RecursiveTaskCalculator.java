package nu.helmers.courses.parallel;

import java.util.concurrent.RecursiveTask;

/**
 * Calculates the sum of all elements in an array.
 * <p/>
 * This implementation uses RecursiveTask from the Fork/Join framework.
 */
public class RecursiveTaskCalculator extends RecursiveTask<Long> {

    private long[] array;
    int fromIx;
    int toIx;

    private static final long THRESHOLD = 50;

    public RecursiveTaskCalculator(long[] array) {
        this(array, 0, array.length);
    }

    /** Notice that in this implementation we use a different (private) constructor for subtasks. */
    private RecursiveTaskCalculator(long[] array, int fromIx, int toIx) {
        this.array = array;
        this.fromIx = fromIx;
        this.toIx = toIx;
    }

    @Override
    protected Long compute() {
        int length = toIx - fromIx;

        // If the task is small enough, do the calculation right away.
        if (length < THRESHOLD) {
            return calculateSum();
        }

        // If the task is too large, split it up in subtasks and execute them in parallel.
        RecursiveTaskCalculator task1 = new RecursiveTaskCalculator(array, fromIx, fromIx + length/2);
        RecursiveTaskCalculator task2 = new RecursiveTaskCalculator(array, fromIx + length/2, toIx);

        task1.fork();                   // Execute in parallel (start future)
        Long result2 = task2.compute(); // Execute synchronously
        Long result1 = task1.join();    // Wait until completed (future.get())

        return result1 + result2;
    }

    private Long calculateSum() {
        long sum = 0;
        for (int i = fromIx; i < toIx; i++) {
            sum += array[i];
        }
        return sum;
    }
}
