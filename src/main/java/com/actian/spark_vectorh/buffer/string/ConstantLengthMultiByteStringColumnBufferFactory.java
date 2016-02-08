package com.actian.spark_vectorh.buffer.string;

public final class ConstantLengthMultiByteStringColumnBufferFactory extends ByteLengthLimitedStringColumnBufferFactory {

    private static final String CHAR_TYPE_ID = "char";

    @Override
    public boolean supportsColumnType(String type, int precision, int scale, boolean nullable) {
        return type.equalsIgnoreCase(CHAR_TYPE_ID) && precision > 1;
    }
}
