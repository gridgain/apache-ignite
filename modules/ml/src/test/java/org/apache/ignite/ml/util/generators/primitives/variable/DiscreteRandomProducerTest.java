package org.apache.ignite.ml.util.generators.primitives.variable;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class DiscreteRandomProducerTest {
    @Test
    public void testGet() {
        double[] probs = new double[] {0.1, 0.2, 0.3, 0.4};
        DiscreteRandomProducer producer = new DiscreteRandomProducer(0L, probs);

        Map<Integer, Double> counters = new HashMap<>();
        IntStream.range(0, probs.length).forEach(i -> counters.put(i, 0.0));

        final int N = 500000;
        Stream.generate(producer::getInt).limit(N).forEach(i -> counters.put(i, counters.get(i) + 1));
        IntStream.range(0, probs.length).forEach(i -> counters.put(i, counters.get(i) / N));

        for (int i = 0; i < probs.length; i++)
            assertEquals(probs[i], counters.get(i), 0.01);

        assertEquals(probs.length, producer.size());
    }

    @Test
    public void testSeedConsidering() {
        DiscreteRandomProducer producer1 = new DiscreteRandomProducer(0L, 0.1, 0.2, 0.3, 0.4);
        DiscreteRandomProducer producer2 = new DiscreteRandomProducer(0L, 0.1, 0.2, 0.3, 0.4);

        assertEquals(producer1.get(), producer2.get(), 0.0001);
    }

    @Test
    public void testUniformGeneration() {
        int N = 10;
        DiscreteRandomProducer producer = DiscreteRandomProducer.uniform(N);

        Map<Integer, Double> counters = new HashMap<>();
        IntStream.range(0, N).forEach(i -> counters.put(i, 0.0));

        final int sampleSize = 500000;
        Stream.generate(producer::getInt).limit(sampleSize).forEach(i -> counters.put(i, counters.get(i) + 1));
        IntStream.range(0, N).forEach(i -> counters.put(i, counters.get(i) / sampleSize));

        for (int i = 0; i < N; i++)
            assertEquals(1.0 / N, counters.get(i), 0.01);
    }

    @Test
    public void testDistributionGeneration() {
        double[] probs = DiscreteRandomProducer.randomDistribution(5, 0L);
        assertArrayEquals(new double[] {0.23, 0.27, 0.079, 0.19, 0.20}, probs, 0.01);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidDistribution1() {
        new DiscreteRandomProducer(0L, 0.1, 0.2, 0.3, 0.0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidDistribution2() {
        new DiscreteRandomProducer(0L, 0.1, 0.2, 0.3, 1.0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidDistribution3() {
        new DiscreteRandomProducer(0L, 0.1, 0.2, 0.3, 1.0, -0.6);
    }
}
