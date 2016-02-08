package com.actian.spark_vectorh.buffer.singles;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.actian.spark_vectorh.buffer.ColumnBufferFactory;
import com.actian.spark_vectorh.buffer.VectorSink;
import com.actian.spark_vectorh.buffer.ColumnBuffer;

public final class BooleanColumnBufferFactory extends ColumnBufferFactory {

    private static final String BOOLEAN_TYPE_ID = "boolean";

    @Override
    public boolean supportsColumnType(String type, int precision, int scale, boolean nullable) {
        return type.equalsIgnoreCase(BOOLEAN_TYPE_ID);
    }

    @Override
    protected ColumnBuffer<?> createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        return new BooleanColumnBuffer(maxRowCount, name, index, nullable);
    }

    private static final class BooleanColumnBuffer extends ColumnBuffer<Boolean> {

        private static final int BOOLEAN_SIZE = 1;
        private static final byte TRUE = (byte) 1;
        private static final byte FALSE = (byte) 0;

        public BooleanColumnBuffer(int rows, String name, int index, boolean nullable) {
            super(rows, BOOLEAN_SIZE, BOOLEAN_SIZE, name, index, nullable);
        }

        @Override
        protected void bufferNextValue(Boolean source, ByteBuffer buffer) {
            buffer.put(source.booleanValue() ? TRUE : FALSE);
        }

        @Override
        protected void write(VectorSink target, int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException {
            target.writeByteColumn(columnIndex, values, markers);
        }
    }
}
