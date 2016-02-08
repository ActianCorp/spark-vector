package com.actian.spark_vectorh.buffer.timestamp;

import static com.actian.spark_vectorh.buffer.timestamp.TimestampConversion.scaledTimestamp;

import java.math.BigInteger;

import com.actian.spark_vectorh.buffer.timestamp.TimestampConversion.TimestampConverter;

final class TimestampLZColumnFactoryCommons {
    private static final String TIMESTAMP_LZ_TYPE_ID = "timestamp with local time zone";

    private TimestampLZColumnFactoryCommons() {
        throw new IllegalStateException();
    }

    public static boolean isSupportedColumnType(String columnTypeName, int columnScale, int minScale, int maxScale) {
        return columnTypeName.equalsIgnoreCase(TIMESTAMP_LZ_TYPE_ID) && minScale <= columnScale && columnScale <= maxScale;
    }

    public static final class TimestampLZConverter implements TimestampConverter {
        @Override
        public BigInteger convert(long epochSeconds, long subsecNanos, int offsetSeconds, int scale) {
            return scaledTimestamp(epochSeconds, subsecNanos, 0, scale);
        }
    }
}
