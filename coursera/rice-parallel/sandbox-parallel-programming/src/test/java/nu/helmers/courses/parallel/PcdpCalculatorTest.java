package nu.helmers.courses.parallel;

import org.junit.Test;

import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** */
public class PcdpCalculatorTest {

    @Test
    public void test() {
        int[] array = IntStream.rangeClosed(0, 200_000_000).toArray();

        Long result = PcdpCalculator.sum(array);

        assertThat(result, is(20000000100000000L));
    }
}