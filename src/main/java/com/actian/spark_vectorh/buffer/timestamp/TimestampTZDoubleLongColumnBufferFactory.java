package com.actian.spark_vectorh.buffer.timestamp;

import static com.actian.spark_vectorh.buffer.timestamp.TimestampTZColumnFactoryCommons.isSupportedColumnType;

import com.actian.spark_vectorh.buffer.timestamp.TimestampConversion.TimestampConverter;
import com.actian.spark_vectorh.buffer.timestamp.TimestampTZColumnFactoryCommons.TimestampTZConverter;

public final class TimestampTZDoubleLongColumnBufferFactory extends TimestampDoubleLongColumnBufferFactory {
    private static final int MIN_TIMESTAMP_TZ_DOUBLE_LONG_SCALE = 5;
    private static final int MAX_TIMESTAMP_TZ_DOUBLE_LONG_SCALE = 9;

    @Override
    protected final boolean adjustToUTC() {
        return false;
    }

    @Override
    public boolean supportsColumnType(String type, int precision, int scale, boolean nullable) {
        return isSupportedColumnType(type, scale, MIN_TIMESTAMP_TZ_DOUBLE_LONG_SCALE, MAX_TIMESTAMP_TZ_DOUBLE_LONG_SCALE);
    }

    @Override
    protected TimestampConverter createConverter() {
        return new TimestampTZConverter();
    }
}
