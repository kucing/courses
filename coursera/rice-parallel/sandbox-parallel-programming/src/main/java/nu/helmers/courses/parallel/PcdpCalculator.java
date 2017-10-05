package nu.helmers.courses.parallel;

import edu.rice.pcdp.PCDP;

/**
 * Calculates the sum of all elements in an array.
 * <p/>
 * This implementation uses the async/finish paradigm provided by Rice university's PCDP library.
 */
public class PcdpCalculator {

    private static class SumParts {
        long sum1 = 0;
        long sum2 = 0;

        public long getTotal() {
            return sum1 + sum2;
        }
    }

    public static long sum(int[] array) {
        long startTime = System.nanoTime();

        final SumParts parts = new SumParts();

        PCDP.finish(() -> {
            PCDP.async(() -> {
                for (int i = 0; i < array.length / 2; i++) {
                    parts.sum1 += array[i];
                }
            });
            for (int i = array.length / 2; i < array.length; i++) {
                parts.sum2 += array[i];
            }
        });

        long endTime = System.nanoTime();
        System.out.printf("Completed in %8.3f ms with sum = %d",
                (endTime-startTime) / 1e6,
                parts.getTotal());
        return parts.getTotal();
    }
}