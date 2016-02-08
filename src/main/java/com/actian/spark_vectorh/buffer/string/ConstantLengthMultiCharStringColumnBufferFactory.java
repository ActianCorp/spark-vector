package com.actian.spark_vectorh.buffer.string;

public final class ConstantLengthMultiCharStringColumnBufferFactory extends CharLengthLimitedStringColumnBufferFactory {

    private static final String NCHAR_TYPE_ID = "nchar";

    @Override
    public boolean supportsColumnType(String type, int precision, int scale, boolean nullable) {
        return type.equalsIgnoreCase(NCHAR_TYPE_ID) && precision > 1;
    }
}
