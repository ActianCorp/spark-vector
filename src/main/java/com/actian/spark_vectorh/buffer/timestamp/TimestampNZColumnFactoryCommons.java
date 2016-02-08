package com.actian.spark_vectorh.buffer.timestamp;

import static com.actian.spark_vectorh.buffer.timestamp.TimestampConversion.scaledTimestamp;

import java.math.BigInteger;

import com.actian.spark_vectorh.buffer.timestamp.TimestampConversion.TimestampConverter;

final class TimestampNZColumnFactoryCommons {
    private static final String TIMESTAMP_NZ_TYPE_ID_1 = "timestamp";
    private static final String TIMESTAMP_NZ_TYPE_ID_2 = "timestamp without time zone";

    private TimestampNZColumnFactoryCommons() {
        throw new IllegalStateException();
    }

    public static boolean isSupportedColumnType(String columnTypeName, int columnScale, int minScale, int maxScale) {
        return (columnTypeName.equalsIgnoreCase(TIMESTAMP_NZ_TYPE_ID_1) || columnTypeName.equalsIgnoreCase(TIMESTAMP_NZ_TYPE_ID_2)) && minScale <= columnScale
                && columnScale <= maxScale;
    }

    public static final class TimestampNZConverter implements TimestampConverter {
        @Override
        public BigInteger convert(long epochSeconds, long subsecNanos, int offsetSeconds, int scale) {
            return scaledTimestamp(epochSeconds, subsecNanos, offsetSeconds, scale);
        }
    }
}
