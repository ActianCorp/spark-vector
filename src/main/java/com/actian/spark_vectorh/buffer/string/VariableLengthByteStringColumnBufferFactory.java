package com.actian.spark_vectorh.buffer.string;

public final class VariableLengthByteStringColumnBufferFactory extends ByteLengthLimitedStringColumnBufferFactory {

    private static final String VARCHAR_TYPE_ID = "varchar";

    @Override
    public boolean supportsColumnType(String type, int precision, int scale, boolean nullable) {
        return type.equalsIgnoreCase(VARCHAR_TYPE_ID) && precision > 0;
    }
}
