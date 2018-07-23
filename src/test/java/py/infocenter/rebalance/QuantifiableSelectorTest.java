package py.infocenter.rebalance;

import org.junit.Test;
import py.common.struct.Pair;
import py.test.TestBase;

import java.util.Arrays;

import static org.junit.Assert.*;

public class QuantifiableSelectorTest extends TestBase {

    @Test
    public void selectTheMinAndMax() throws Exception {
        QuantifiableSelector<Integer> selector = new QuantifiableSelector<>(value -> value);
        Pair<Pair<Integer, Double>, Pair<Integer, Double>> result = selector
                .selectTheMinAndMax(Arrays.asList(15, 10, 10, 10, 1), 0.1);
        assertEquals(result.getFirst().getFirst().intValue(), 1);
        assertEquals(result.getSecond().getFirst().intValue(), 15);

        result = selector.selectTheMinAndMax(Arrays.asList(15, 10, 10, 10, 10), 0.2);
        assertNull(result.getFirst());
        assertEquals(result.getSecond().getFirst().intValue(), 15);

        result = selector.selectTheMinAndMax(Arrays.asList(15, 10, 10, 10, 10), 0.8);
        assertNull(result.getFirst());
        assertNull(result.getSecond());

        result = selector.selectTheMinAndMax(Arrays.asList(10, 10, 10, 10, 10), 0.01);
        assertNull(result.getFirst());
        assertNull(result.getSecond());

        double val1 = selector.selectTheMinAndMax(Arrays.asList(10, 10, 10, 10, 15), 0.01).getSecond().getSecond();
        double val2 = selector.selectTheMinAndMax(Arrays.asList(10, 10, 10, 10, 16), 0.01).getSecond().getSecond();
        double val3 = selector.selectTheMinAndMax(Arrays.asList(10, 10, 10, 10, 17), 0.01).getSecond().getSecond();
        double val4 = selector.selectTheMinAndMax(Arrays.asList(10, 10, 10, 10, 18), 0.01).getSecond().getSecond();
        assertTrue(val4 > val3);
        assertTrue(val3 > val2);
        assertTrue(val2 > val1);

        val1 = selector.selectTheMinAndMax(Arrays.asList(10, 10, 10, 10, 5), 0.01).getFirst().getSecond();
        val2 = selector.selectTheMinAndMax(Arrays.asList(10, 10, 10, 10, 6), 0.01).getFirst().getSecond();
        val3 = selector.selectTheMinAndMax(Arrays.asList(10, 10, 10, 10, 7), 0.01).getFirst().getSecond();
        val4 = selector.selectTheMinAndMax(Arrays.asList(10, 10, 10, 10, 8), 0.01).getFirst().getSecond();
        assertTrue(val4 < val3);
        assertTrue(val3 < val2);
        assertTrue(val2 < val1);

        val1 = selector.selectTheMinAndMax(Arrays.asList(10, 10, 10, 10, 5), 0.01).getFirst().getSecond();
        val2 = selector.selectTheMinAndMax(Arrays.asList(10, 10, 10, 10, 16), 0.01).getSecond().getSecond();
        val3 = selector.selectTheMinAndMax(Arrays.asList(10, 10, 10, 10, 3), 0.01).getFirst().getSecond();
        val4 = selector.selectTheMinAndMax(Arrays.asList(10, 10, 10, 10, 17), 0.01).getSecond().getSecond();
        assertTrue(val2 > val1);
        assertTrue(val3 > val2);
        assertTrue(val3 == val4);
    }

}