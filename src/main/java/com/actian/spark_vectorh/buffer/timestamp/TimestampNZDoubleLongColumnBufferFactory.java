package com.actian.spark_vectorh.buffer.timestamp;

import static com.actian.spark_vectorh.buffer.timestamp.TimestampNZColumnFactoryCommons.isSupportedColumnType;

import com.actian.spark_vectorh.buffer.timestamp.TimestampConversion.TimestampConverter;
import com.actian.spark_vectorh.buffer.timestamp.TimestampNZColumnFactoryCommons.TimestampNZConverter;

public final class TimestampNZDoubleLongColumnBufferFactory extends TimestampDoubleLongColumnBufferFactory {
    private static final int MIN_TIMESTAMP_NZ_DOUBLE_LONG_SCALE = 8;
    private static final int MAX_TIMESTAMP_NZ_DOUBLE_LONG_SCALE = 9;

    @Override
    public boolean supportsColumnType(String type, int precision, int scale, boolean nullable) {
        return isSupportedColumnType(type, scale, MIN_TIMESTAMP_NZ_DOUBLE_LONG_SCALE, MAX_TIMESTAMP_NZ_DOUBLE_LONG_SCALE);
    }

    @Override
    protected TimestampConverter createConverter() {
        return new TimestampNZConverter();
    }
}
