package com.actian.spark_vectorh.buffer.string;

import com.actian.spark_vectorh.buffer.ColumnBuffer;
import com.actian.spark_vectorh.buffer.string.IntegerEncodedStringColumnBufferFactoryCommons.IntegerEncodedStringColumnBuffer;

public final class ConstantLengthSingleCharStringColumnBufferFactory extends StringColumnBufferFactory {

    private static final String NCHAR_TYPE_ID = "nchar";

    @Override
    public boolean supportsColumnType(String type, int precision, int scale, boolean nullable) {
        return type.equalsIgnoreCase(NCHAR_TYPE_ID) && precision == 1;
    }

    @Override
    protected ColumnBuffer<?> createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        return new ConstantLengthSingleCharStringColumnBuffer(maxRowCount, name, index, nullable);
    }

    private static final class ConstantLengthSingleCharStringColumnBuffer extends IntegerEncodedStringColumnBuffer {

        public ConstantLengthSingleCharStringColumnBuffer(int rows, String name, int index, boolean nullable) {
            super(rows, name, index, nullable);
        }

        @Override
        protected int encodeNextValue(String value) {
            if (Character.isHighSurrogate(value.charAt(0))) {
                return WHITESPACE;
            } else {
                return value.codePointAt(0);
            }
        }
    }
}
