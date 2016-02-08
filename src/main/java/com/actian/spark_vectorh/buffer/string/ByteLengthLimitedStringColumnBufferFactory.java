package com.actian.spark_vectorh.buffer.string;

import com.actian.spark_vectorh.buffer.ColumnBuffer;
import com.actian.spark_vectorh.buffer.string.ByteEncodedStringColumnBufferFactoryCommons.ByteEncodedStringColumnBuffer;

abstract class ByteLengthLimitedStringColumnBufferFactory extends StringColumnBufferFactory {

    @Override
    protected final ColumnBuffer<?> createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        return new ByteLimitedByteStringColumnBuffer(maxRowCount, name, index, precision, nullable);
    }

    private static final class ByteLimitedByteStringColumnBuffer extends ByteEncodedStringColumnBuffer {

        private final int precision;

        public ByteLimitedByteStringColumnBuffer(int rows, String name, int index, int precision, boolean nullable) {
            super(rows, name, index, precision, nullable);
            this.precision = precision;
        }

        @Override
        protected byte[] encodeNextValue(String value) {
            return StringConversion.truncateToUTF8Bytes(value, precision);
        }
    }
}
