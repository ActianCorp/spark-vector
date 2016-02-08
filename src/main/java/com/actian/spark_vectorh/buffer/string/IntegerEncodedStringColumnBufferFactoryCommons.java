package com.actian.spark_vectorh.buffer.string;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.actian.spark_vectorh.buffer.VectorSink;
import com.actian.spark_vectorh.buffer.ColumnBuffer;

final class IntegerEncodedStringColumnBufferFactoryCommons {

    private IntegerEncodedStringColumnBufferFactoryCommons() {
        throw new IllegalStateException();
    }

    public static abstract class IntegerEncodedStringColumnBuffer extends ColumnBuffer<String> {

        private static final int CHAR_SIZE = 4;
        protected static final char WHITESPACE = '\u0020';

        protected IntegerEncodedStringColumnBuffer(int rows, String name, int index, boolean nullable) {
            super(rows, CHAR_SIZE, CHAR_SIZE, name, index, nullable);
        }

        @Override
        protected void bufferNextValue(String source, ByteBuffer buffer) {
            String value = source;
            if (value.isEmpty()) {
                buffer.putInt(WHITESPACE);
            } else {
                buffer.putInt(encodeNextValue(value));
            }
        }

        @Override
        protected void write(VectorSink target, int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException {
            target.writeIntColumn(columnIndex, values, markers);
        }

        protected abstract int encodeNextValue(String value);
    }
}
