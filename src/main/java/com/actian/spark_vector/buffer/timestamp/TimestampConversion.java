/*
 * Copyright 2016 Actian Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.actian.spark_vector.buffer.timestamp;

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
