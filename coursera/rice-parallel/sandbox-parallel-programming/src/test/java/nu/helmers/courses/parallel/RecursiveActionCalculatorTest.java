package nu.helmers.courses.parallel;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.LongStream;

/**
 * This test illustrates how to execute a fixed number of (non-recursive) {@link java.util.concurrent.RecursiveAction}s
 * in parallel:
 * <p/>
 *
 * <pre>
 *     // First create all actions to be executed.
 *
 *     // Then submit them to a pool, join them, and calculate the end result.
 *     actions.stream().forEach(a -> forkJoinPool.submit(a));
 *     actions.stream().forEach(a -> a.join());
 *     result = actions.stream().mapToDouble(a -> a.getResult()).sum();
 * </pre>
 *
 * Note that (in this example) the action itself does not split up into sub-actions; it is non-recursive!
 */
public class RecursiveActionCalculatorTest {

    @Test
    public void test() {
        long[] array = LongStream.rangeClosed(1, 200_000_000).toArray();

        double result1 = timedCalculateSum(array, 1);
        double result2 = timedCalculateSum(array, 4);

        Assert.assertEquals(result1, result2, 0);
    }

    private double calculateSum(final long[] input, final int numTasks) {
        // Split up the array in chunks,
        // and create one task for each chunk.
        List<int[]> chunks = Arrays.asList(getChunks(input, numTasks));
        Collection<RecursiveActionCalculator> actions = new ArrayList<>();
        chunks.stream().forEach(c -> actions.add(new RecursiveActionCalculator(c[0], c[1], input)));

        // Submit all tasks to a fork/join pool.
        // Simply doing "task.fork()" won't give you as much speedup! Always use a pool!
        ForkJoinPool pool = new ForkJoinPool(numTasks);
        actions.stream().forEach(a -> pool.submit(a));
        actions.stream().forEach(a -> a.join());

        // Since we're using a RecursiveAction (as opposed to RecursiveTask),
        // we have to get the results from the RecursiveActions 'manually'.
        return actions.stream().mapToDouble(a -> a.getResult()).sum();
    }

    private double timedCalculateSum(long[] array, int parallelism) {
        long startTime = System.nanoTime();
        double result = calculateSum(array, parallelism);
        long endTime = System.nanoTime();
        System.out.println("Duration (parallelism=" + parallelism + "): " + (endTime - startTime) / 1000000 + "ms");
        return result;
    }

    // Helper methods below were copied from Rice university's coursera course 'Parallel Programming in Java'.

    /**
     * Computes the size of each chunk, given the number of chunks to create
     * across a given number of elements.
     *
     * @param nChunks The number of chunks to create
     * @param nElements The number of elements to chunk across
     * @return The default chunk size
     */
    private static int getChunkSize(final int nChunks, final int nElements) {
        // Integer ceil
        return (nElements + nChunks - 1) / nChunks;
    }

    /**
     * Computes the inclusive element index that the provided chunk starts at,
     * given there are a certain number of chunks.
     *
     * @param chunk The chunk to compute the start of
     * @param nChunks The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The inclusive index that this chunk starts at in the set of
     *         nElements
     */
    private static int getChunkStartInclusive(final int chunk, final int nChunks, final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        return chunk * chunkSize;
    }

    /**
     * Computes the exclusive element index that the provided chunk ends at,
     * given there are a certain number of chunks.
     *
     * @param chunk The chunk to compute the end of
     * @param nChunks The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The exclusive end index for this chunk
     */
    private static int getChunkEndExclusive(final int chunk, final int nChunks, final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        final int end = (chunk + 1) * chunkSize;
        if (end > nElements) {
            return nElements;
        } else {
            return end;
        }
    }

    protected static int[][] getChunks(final long[] input, final int numChunks) {
        int[][] chunks = new int[numChunks][2];
        for (int i = 0; i< numChunks; i++) {
            int startInclusive = getChunkStartInclusive(i, numChunks, input.length);
            int endExclusive = getChunkEndExclusive(i, numChunks, input.length);
            chunks[i] = new int[] {startInclusive, endExclusive};
        }
        return chunks;
    }
}