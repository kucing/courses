package nu.helmers.courses.parallel;

import org.junit.Test;

import java.util.concurrent.ForkJoinPool;
import java.util.stream.LongStream;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.*;

/**
 * This test illustrates how to execute a program in parallel, with recursion, using
 * {@link java.util.concurrent.RecursiveTask}:
 * <p/>
 *
 * <pre>
 *     result = forkJoinPool.invoke(recursiveTask);
 * </pre>
 *
 * The <code>recursiveTask</code> is responsible fo calculating the end result. It does so using recursion.
 * <p/>
 * Before scheduling your parallel program, you should determine (using a computation graph) the maximum possible
 * parallelism, i.e. the maximum speedup you can obtain with an unbounded number of processors.
 */
public class RecursiveTaskCalculatorTest {

    @Test
    public void test() {
        long[] array = LongStream.rangeClosed(0, 200_000_000).toArray();

        // This system property sets the degree of parallelism you want to exploit when using the common pool
        // from the Fork/Join framework.
        // Without this system property the common pool sets parallelism = Runtime.getRuntime().availableProcessors()-1.
        // Because there's always a main thread, there's no point in setting the parallelism higher than this.
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "4");

        Long result = calculateSum(array, 50);

        assertThat(result, is(20000000100000000L));
    }

    private Long calculateSum(long[] array, int times) {
        Long result = 0l;

        for (int i = 0; i < times; i++) {
            RecursiveTaskCalculator task = new RecursiveTaskCalculator(array);
            long startTime = System.nanoTime();

            // You can use the common pool...
            result = ForkJoinPool.commonPool().invoke(task);
            // Or a custom pool, passing in the desired degree of parallelism.
//            result = new ForkJoinPool(4).invoke(task);

            System.out.printf("Completed in %8.3f ms \n", (System.nanoTime() - startTime) / 1e6);
        }

        return result; // just return the most recent result...
    }
}