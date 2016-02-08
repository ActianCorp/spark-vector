package com.actian.spark_vectorh.buffer.string;

public final class VariableLengthCharStringColumnBufferFactory extends CharLengthLimitedStringColumnBufferFactory {

    private static final String NVARCHAR_TYPE_ID = "nvarchar";

    @Override
    public boolean supportsColumnType(String type, int precision, int scale, boolean nullable) {
        return type.equalsIgnoreCase(NVARCHAR_TYPE_ID) && precision > 0;
    }
}
