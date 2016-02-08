package com.actian.spark_vectorh.buffer.time;

import static com.actian.spark_vectorh.buffer.time.TimeConversion.scaledTime;

import com.actian.spark_vectorh.buffer.time.TimeConversion.TimeConverter;

final class TimeNZColumnFactoryCommons {
    private static final String TIME_NZ_TYPE_ID_1 = "time";
    private static final String TIME_NZ_TYPE_ID_2 = "time without time zone";

    private TimeNZColumnFactoryCommons() {
        throw new IllegalStateException();
    }

    public static boolean isSupportedColumnType(String columnTypeName, int columnScale, int minScale, int maxScale) {
        return (columnTypeName.equalsIgnoreCase(TIME_NZ_TYPE_ID_1) || columnTypeName.equalsIgnoreCase(TIME_NZ_TYPE_ID_2)) && minScale <= columnScale
                && columnScale <= maxScale;
    }

    public static final class TimeNZConverter extends TimeConverter {
        @Override
        public long convert(long nanos, int scale) {
            return scaledTime(nanos, scale);
        }
    }
}
