package co.edu.unal.paralela;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.logging.Logger;

public final class ReciprocalArraySum extends RecursiveAction {

    private static final Logger log = Logger.getLogger(ReciprocalArraySum.class.getName());

    /*
     * Pool compartido para evitar crear un ForkJoinPool nuevo en cada llamada.
     * Esto ayuda mucho en los tests de rendimiento.
     */
    private static final ForkJoinPool FORK_JOIN_POOL =
            new ForkJoinPool(Runtime.getRuntime().availableProcessors());

    private double[] array;
    private int p;
    private int r;
    private double value;

    private ReciprocalArraySum(double[] array, int p, int r) {
        this.array = array;
        this.p = p;
        this.r = r;
    }

    protected static double seqArraySum(final double[] input) {
        double sum = 0;

        for (int i = 0; i < input.length; i++) {
            sum += 1 / input[i];
        }

        return sum;
    }

    private static int getChunkSize(final int nChunks, final int nElements) {
        return (nElements + nChunks - 1) / nChunks;
    }

    private static int getChunkStartInclusive(final int chunk,
            final int nChunks, final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        return chunk * chunkSize;
    }

    private static int getChunkEndExclusive(final int chunk,
            final int nChunks, final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        final int end = (chunk + 1) * chunkSize;

        if (end > nElements) {
            return nElements;
        } else {
            return end;
        }
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
                localSum += 1 / input[i];
            }

            value = localSum;
        }
    }

    protected static double parArraySum(final double[] input) {

        assert input.length % 2 == 0;

        final int mid = input.length / 2;

        final ReciprocalArraySumTask leftTask =
                new ReciprocalArraySumTask(0, mid, input);

        final ReciprocalArraySumTask rightTask =
                new ReciprocalArraySumTask(mid, input.length, input);

        FORK_JOIN_POOL.invoke(new RecursiveAction() {
            @Override
            protected void compute() {
                invokeAll(leftTask, rightTask);
            }
        });

        return leftTask.getValue() + rightTask.getValue();
    }

    @Override
    protected void compute() {
        double sum0 = 0;
        double sum1 = 0;
        double sum2 = 0;
        double sum3 = 0;
        double sum4 = 0;
        double sum5 = 0;
        double sum6 = 0;
        double sum7 = 0;

        int i = startIndexInclusive;

        final int limit = endIndexExclusive - 7;

        for (; i < limit; i += 8) {
        sum0 += 1.0 / input[i];
        sum1 += 1.0 / input[i + 1];
        sum2 += 1.0 / input[i + 2];
        sum3 += 1.0 / input[i + 3];
        sum4 += 1.0 / input[i + 4];
        sum5 += 1.0 / input[i + 5];
        sum6 += 1.0 / input[i + 6];
        sum7 += 1.0 / input[i + 7];
        }

        double localSum = sum0 + sum1 + sum2 + sum3
            + sum4 + sum5 + sum6 + sum7;

        for (; i < endIndexExclusive; i++) {
        localSum += 1.0 / input[i];
        }

        value = localSum;
}

    protected static double parManyTaskArraySum(final double[] input,
            final int numTasks) {

        final ReciprocalArraySumTask[] tasks =
                new ReciprocalArraySumTask[numTasks];

        for (int i = 0; i < numTasks; i++) {
            final int start = getChunkStartInclusive(i, numTasks, input.length);
            final int end = getChunkEndExclusive(i, numTasks, input.length);

            tasks[i] = new ReciprocalArraySumTask(start, end, input);
        }

        FORK_JOIN_POOL.invoke(new RecursiveAction() {
            @Override
            protected void compute() {
                invokeAll(tasks);
            }
        });

        double sum = 0;

        for (int i = 0; i < numTasks; i++) {
            sum += tasks[i].getValue();
        }

        return sum;
    }

    public static void main(String[] args) {

        double[] array = new double[1_000_000];

        for (int i = 0; i < array.length; i++) {
            array[i] = i + 1;
        }

        log.info("Aqui inicia el proceso");
    }
}