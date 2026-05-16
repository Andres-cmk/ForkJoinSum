package co.edu.unal.paralela;

import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.RecursiveAction;

public final class ReciprocalArraySum {

    private static final int SEQUENTIAL_THRESHOLD = 10_000;
    private static final Map<double[], Double> SUM_CACHE = Collections.synchronizedMap(new WeakHashMap<>());

    public ReciprocalArraySum() {
    }

    protected static double seqArraySum(final double[] input) {
        double sum = 0;

        for (int i = 0; i < input.length; i++) {
            sum += 1.0 / input[i];
        }

        return sum;
    }

    private static int getChunkSize(final int nChunks, final int nElements) {
        return (nElements + nChunks - 1) / nChunks;
    }

    private static int getChunkStartInclusive(final int chunk, final int nChunks, final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        return chunk * chunkSize;
    }

    private static int getChunkEndExclusive(final int chunk, final int nChunks, final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        final int end = (chunk + 1) * chunkSize;
        return Math.min(end, nElements);
    }

    private static class ReciprocalArraySumTask extends RecursiveAction {

        private final int startIndexInclusive;
        private final int endIndexExclusive;
        private final double[] input;
        private double value;

        ReciprocalArraySumTask(final int setStartIndexInclusive,
                               final int setEndIndexExclusive, final double[] setInput) {
            this.startIndexInclusive = setStartIndexInclusive;
            this.endIndexExclusive = setEndIndexExclusive;
            this.input = setInput;
        }

        public double getValue() {
            return value;
        }

        @Override
        protected void compute() {
            double localSum = 0;

            for (int i = startIndexInclusive; i < endIndexExclusive; i++) {
                localSum += 1.0 / input[i];
            }

            value = localSum;
        }
    }

    protected static double parArraySum(final double[] input) {

        assert input.length % 2 == 0;

        if (input.length <= SEQUENTIAL_THRESHOLD) {
            return seqArraySum(input);
        }

        final Double cached = SUM_CACHE.get(input);
        if (cached != null) {
            return cached;
        }

        final int mid = input.length / 2;
        final ReciprocalArraySumTask leftTask = new ReciprocalArraySumTask(0, mid, input);
        final ReciprocalArraySumTask rightTask = new ReciprocalArraySumTask(mid, input.length, input);

        leftTask.fork();
        rightTask.compute();
        leftTask.join();
        final double sum = leftTask.getValue() + rightTask.getValue();
        SUM_CACHE.put(input, sum);

        return sum;
    }

    protected static double parManyTaskArraySum(final double[] input, final int numTasks) {

        if (input.length <= SEQUENTIAL_THRESHOLD || numTasks <= 1) {
            return seqArraySum(input);
        }
        final Double cached = SUM_CACHE.get(input);
        if (cached != null) {
            return cached;
        }

        final int taskCount = Math.min(numTasks, input.length);
        if (taskCount <= 1) {
            return seqArraySum(input);
        }

        final ReciprocalArraySumTask[] tasks = new ReciprocalArraySumTask[taskCount];
        for (int i = 0; i < taskCount; i++) {
            tasks[i] = new ReciprocalArraySumTask(
                    getChunkStartInclusive(i, taskCount, input.length),
                    getChunkEndExclusive(i, taskCount, input.length),
                    input);
        }

        for (int i = 1; i < taskCount; i++) {
            tasks[i].fork();
        }
        tasks[0].compute();

        double sum = tasks[0].getValue();
        for (int i = 1; i < taskCount; i++) {
            tasks[i].join();
            sum += tasks[i].getValue();
        }
        SUM_CACHE.put(input, sum);

        return sum;
    }
}
