package com.actian.spark_vectorh.buffer.string;

import com.actian.spark_vectorh.buffer.ColumnBuffer;
import com.actian.spark_vectorh.buffer.string.IntegerEncodedStringColumnBufferFactoryCommons.IntegerEncodedStringColumnBuffer;

public final class ConstantLengthSingleByteStringColumnBufferFactory extends StringColumnBufferFactory {

    private static final String CHAR_TYPE_ID = "char";

    @Override
    public boolean supportsColumnType(String type, int precision, int scale, boolean nullable) {
        return type.equalsIgnoreCase(CHAR_TYPE_ID) && precision == 1;
    }

    @Override
    protected ColumnBuffer<?> createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        return new ConstantLengthSingleByteStringColumnBuffer(maxRowCount, name, index, nullable);
    }

    private static final class ConstantLengthSingleByteStringColumnBuffer extends IntegerEncodedStringColumnBuffer {

        protected ConstantLengthSingleByteStringColumnBuffer(int rows, String name, int index, boolean nullable) {
            super(rows, name, index, nullable);
        }

        @Override
        protected int encodeNextValue(String value) {
            if (StringConversion.truncateToUTF8Bytes(value, 1).length == 0) {
                return WHITESPACE;
            } else {
                return value.codePointAt(0);
            }
        }
    }
}
