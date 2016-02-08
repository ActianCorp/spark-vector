package com.actian.spark_vectorh.buffer.time;

import static com.actian.spark_vectorh.buffer.time.TimeConversion.normalizedTime;
import static com.actian.spark_vectorh.buffer.time.TimeConversion.scaledTime;

import java.util.Calendar;
import java.util.Date;

import com.actian.spark_vectorh.buffer.time.TimeConversion.TimeConverter;

final class TimeLZColumnFactoryCommons {
    private static final String TIME_LZ_TYPE_ID = "time with local time zone";

    private TimeLZColumnFactoryCommons() {
        throw new IllegalStateException();
    }

    public static boolean isSupportedColumnType(String columnTypeName, int columnScale, int minScale, int maxScale) {
        return columnTypeName.equalsIgnoreCase(TIME_LZ_TYPE_ID) && minScale <= columnScale && columnScale <= maxScale;
    }

    public static final class TimeLZConverter extends TimeConverter {

        @Override
        public long convert(long nanos, int scale) {
            return scaledTime(nanos, scale);
        }
    }
}
