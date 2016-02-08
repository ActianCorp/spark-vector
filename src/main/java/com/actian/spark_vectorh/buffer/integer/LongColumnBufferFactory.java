package com.actian.spark_vectorh.buffer.integer;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.actian.spark_vectorh.buffer.ColumnBufferFactory;
import com.actian.spark_vectorh.buffer.VectorSink;
import com.actian.spark_vectorh.buffer.ColumnBuffer;

public final class LongColumnBufferFactory extends ColumnBufferFactory {

    private static final String LONG_TYPE_ID_1 = "bigint";
    private static final String LONG_TYPE_ID_2 = "integer8";

    @Override
    public boolean supportsColumnType(String type, int precision, int scale, boolean nullable) {
        return type.equalsIgnoreCase(LONG_TYPE_ID_1) || type.equalsIgnoreCase(LONG_TYPE_ID_2);
    }

    @Override
    protected ColumnBuffer<?> createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        return new LongColumnBuffer(maxRowCount, name, index, nullable);
    }

    private static final class LongColumnBuffer extends ColumnBuffer<Long> {

        private static final int LONG_SIZE = 8;

        public LongColumnBuffer(int rows, String name, int index, boolean nullable) {
            super(rows, LONG_SIZE, LONG_SIZE, name, index, nullable);
        }

        @Override
        protected void bufferNextValue(Long source, ByteBuffer buffer) {
            buffer.putLong(source);
        }

        @Override
        protected void write(VectorSink target, int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException {
            target.writeLongColumn(columnIndex, values, markers);
        }
    }
}
