package com.actian.spark_vectorh.buffer.time;

import static com.actian.spark_vectorh.buffer.time.TimeTZColumnFactoryCommons.isSupportedColumnType;

import com.actian.spark_vectorh.buffer.time.TimeConversion.TimeConverter;
import com.actian.spark_vectorh.buffer.time.TimeTZColumnFactoryCommons.TimeTZConverter;

public final class TimeTZLongColumnBufferFactory extends TimeLongColumnBufferFactory {
    private static final int MIN_TIME_LONG_TZ_SCALE = 2;
    private static final int MAX_TIME_LONG_TZ_SCALE = 9;

    @Override
    protected final boolean adjustToUTC() {
        return false;
    }

    @Override
    public boolean supportsColumnType(String type, int precision, int scale, boolean nullable) {
        return isSupportedColumnType(type, scale, MIN_TIME_LONG_TZ_SCALE, MAX_TIME_LONG_TZ_SCALE);
    }

    @Override
    protected TimeConverter createConverter() {
        return new TimeTZConverter();
    }
}
