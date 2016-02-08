package com.actian.spark_vectorh.buffer.timestamp;

import java.math.BigInteger;

final class TimestampConversion {
    private static final long SECONDS_BEFORE_EPOCH = 62167219200L;
    private static final BigInteger SECONDS_BEFORE_EPOCH_BI = BigInteger.valueOf(SECONDS_BEFORE_EPOCH);
    private static final int NANOSECONDS_SCALE = 9;
    private static final BigInteger NANOSECONDS_FACTOR_BI = BigInteger.valueOf((long) Math.pow(10, NANOSECONDS_SCALE));

    private TimestampConversion() {
        throw new IllegalStateException();
    }

    public static interface TimestampConverter {
        public BigInteger convert(long epochSeconds, long subsecNanos, int offsetSeconds, int scale);
    }

    public static BigInteger scaledTimestamp(long epochSeconds, long subsecNanos, int offsetSeconds, int scale) {
        BigInteger secondsTotal = BigInteger.valueOf(epochSeconds).add(BigInteger.valueOf(offsetSeconds)).add(SECONDS_BEFORE_EPOCH_BI);
        BigInteger nanosTotal = secondsTotal.multiply(NANOSECONDS_FACTOR_BI).add(BigInteger.valueOf(subsecNanos));
        int adjustment = scale - NANOSECONDS_SCALE;
        BigInteger adjustmentFactor = BigInteger.valueOf((long) Math.pow(10, Math.abs(adjustment)));
        return adjustment >= 0 ? nanosTotal.multiply(adjustmentFactor) : nanosTotal.divide(adjustmentFactor);
    }
}
