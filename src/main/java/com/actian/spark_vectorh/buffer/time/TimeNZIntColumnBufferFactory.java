package com.actian.spark_vectorh.buffer.time;

import static com.actian.spark_vectorh.buffer.time.TimeNZColumnFactoryCommons.isSupportedColumnType;

import com.actian.spark_vectorh.buffer.time.TimeConversion.TimeConverter;
import com.actian.spark_vectorh.buffer.time.TimeNZColumnFactoryCommons.TimeNZConverter;

public final class TimeNZIntColumnBufferFactory extends TimeIntColumnBufferFactory {
    private static final int MIN_TIME_INT_NZ_SCALE = 0;
    private static final int MAX_TIME_INT_NZ_SCALE = 4;

    @Override
    public boolean supportsColumnType(String type, int precision, int scale, boolean nullable) {
        return isSupportedColumnType(type, scale, MIN_TIME_INT_NZ_SCALE, MAX_TIME_INT_NZ_SCALE);
    }

    @Override
    protected TimeConverter createConverter() {
        return new TimeNZConverter();
    }
}
