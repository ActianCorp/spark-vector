package com.actian.spark_vectorh.buffer.string;

import com.actian.spark_vectorh.buffer.ColumnBuffer;
import com.actian.spark_vectorh.buffer.string.ByteEncodedStringColumnBufferFactoryCommons.ByteEncodedStringColumnBuffer;

abstract class CharLengthLimitedStringColumnBufferFactory extends StringColumnBufferFactory {

    @Override
    protected final ColumnBuffer<?> createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        return new CharLimitedStringColumnBuffer(maxRowCount, name, index, precision, nullable);
    }

    private static final class CharLimitedStringColumnBuffer extends ByteEncodedStringColumnBuffer {

        private static final int MAX_UTF_8_CHAR_SIZE = 4;

        private final int precision;

        public CharLimitedStringColumnBuffer(int rows, String name, int index, int precision, boolean nullable) {
            super(rows, name, index, precision * MAX_UTF_8_CHAR_SIZE, nullable);
            this.precision = precision;
        }

        @Override
        protected byte[] encodeNextValue(String value) {
            return StringConversion.truncateToUTF16CodeUnits(value, precision);
        }
    }
}
