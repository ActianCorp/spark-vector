package com.actian.spark_vectorh.buffer.integer;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.actian.spark_vectorh.buffer.ColumnBufferFactory;
import com.actian.spark_vectorh.buffer.VectorSink;
import com.actian.spark_vectorh.buffer.ColumnBuffer;

public final class IntColumnBufferFactory extends ColumnBufferFactory {

    private static final String INT_TYPE_ID_1 = "integer";
    private static final String INT_TYPE_ID_2 = "integer4";

    @Override
    public boolean supportsColumnType(String type, int precision, int scale, boolean nullable) {
        return type.equalsIgnoreCase(INT_TYPE_ID_1) || type.equalsIgnoreCase(INT_TYPE_ID_2);
    }

    @Override
    protected ColumnBuffer<?> createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        return new IntColumnBuffer(maxRowCount, name, index, nullable);
    }

    private static final class IntColumnBuffer extends ColumnBuffer<Integer> {

        private static final int INT_SIZE = 4;

        public IntColumnBuffer(int rows, String name, int index, boolean nullable) {
            super(rows, INT_SIZE, INT_SIZE, name, index, nullable);
        }

        @Override
        protected void bufferNextValue(Integer source, ByteBuffer buffer) {
            buffer.putInt(source);
        }

        @Override
        protected void write(VectorSink target, int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException {
            target.writeIntColumn(columnIndex, values, markers);
        }
    }
}
