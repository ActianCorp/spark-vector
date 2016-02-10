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

import static com.actian.spark_vector.buffer.timestamp.TimestampConversion.scaledTimestamp;

import java.math.BigInteger;

import com.actian.spark_vector.buffer.timestamp.TimestampConversion.TimestampConverter;

final class TimestampTZColumnFactoryCommons {
    private static final String TIMESTAMP_TZ_TYPE_ID = "timestamp with time zone";

    private static final long SECONDS_IN_MINUTE = 60;

    private TimestampTZColumnFactoryCommons() {
        throw new IllegalStateException();
    }

    public static boolean isSupportedColumnType(String columnTypeName, int columnScale, int minScale, int maxScale) {
        return columnTypeName.equalsIgnoreCase(TIMESTAMP_TZ_TYPE_ID) && minScale <= columnScale && columnScale <= maxScale;
    }

    public static final class TimestampTZConverter implements TimestampConverter {
        private static final BigInteger TIME_MASK = new BigInteger(timeMask());
        private static final BigInteger ZONE_MASK = BigInteger.valueOf(0x7FF);

        @Override
        public BigInteger convert(long epochSeconds, long subsecNanos, int offsetSeconds, int scale) {
            BigInteger scaledTimestamp = scaledTimestamp(epochSeconds, subsecNanos, 0, scale);
            return scaledTimestamp.shiftLeft(11).and(TIME_MASK).or(BigInteger.valueOf(offsetSeconds / SECONDS_IN_MINUTE).and(ZONE_MASK));
        }

        // Set the 117 most significant bits to 1 and the 11 least significant
        // bits to 0.
        private static byte[] timeMask() {
            byte[] mask = new byte[16];
            mask[0] = 0;
            mask[1] = (byte) 248;
            for (int i = 2; i < mask.length; i++) {
                mask[i] = (byte) 0xFF;
            }
            return mask;
        }
    }
}
