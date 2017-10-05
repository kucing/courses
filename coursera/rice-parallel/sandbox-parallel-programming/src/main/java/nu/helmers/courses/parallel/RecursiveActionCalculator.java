package nu.helmers.courses.parallel;

import java.util.concurrent.RecursiveAction;

/**
 * Calculates the sum of all elements in an array.
 * <p/>
 * This implementation uses RecursiveAction from the Fork/Join framework.
 * <p/>
 * It uses no recursion, and is designed for use with a fixed number of subtasks. It expects the caller to split up the
 * calculation in feasible subtasks (segments, defined by the start and end index passed in to the constructor).
 */
public class RecursiveActionCalculator extends RecursiveAction {

    private final long[] input;
    private final int startIndexInclusive;
    private final int endIndexExclusive;
    private long result;

    public RecursiveActionCalculator(final int setStartIndexInclusive,
                                     final int setEndIndexExclusive,
                                     final long[] setInput) {
        this.startIndexInclusive = setStartIndexInclusive;
        this.endIndexExclusive = setEndIndexExclusive;
        this.input = setInput;
    }

    public long getResult() {
        return result;
    }

    @Override
    protected void compute() {
        long sum = 0;
        for (int i = startIndexInclusive; i < endIndexExclusive; i++) {
            sum += input[i];
        }
        this.result = sum;
    }
}
