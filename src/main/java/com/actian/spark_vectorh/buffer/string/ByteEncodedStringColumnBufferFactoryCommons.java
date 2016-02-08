package com.actian.spark_vectorh.buffer.string;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.actian.spark_vectorh.buffer.VectorSink;
import com.actian.spark_vectorh.buffer.ColumnBuffer;

final class ByteEncodedStringColumnBufferFactoryCommons {

    private ByteEncodedStringColumnBufferFactoryCommons() {
        throw new IllegalStateException();
    }

    public static abstract class ByteEncodedStringColumnBuffer extends ColumnBuffer<String> {
        public ByteEncodedStringColumnBuffer(int rows, String name, int index, int precision, boolean nullable) {
            // Additional 0-byte.
            super(rows, precision + 1, 1, name, index, nullable);
        }

        @Override
        protected void bufferNextValue(String source, ByteBuffer buffer) {
            byte[] bytes = encodeNextValue(source);
            buffer.put(bytes);
            buffer.put((byte) 0);
        }

        @Override
        protected void write(VectorSink target, int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException {
            target.writeStringColumn(columnIndex, values, markers);
        }

        protected abstract byte[] encodeNextValue(String value);
    }

}
