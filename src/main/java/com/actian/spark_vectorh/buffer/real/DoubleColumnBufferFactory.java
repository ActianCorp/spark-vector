package com.actian.spark_vectorh.buffer.real;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.actian.spark_vectorh.buffer.VectorSink;
import com.actian.spark_vectorh.buffer.ColumnBuffer;
import com.actian.spark_vectorh.buffer.ColumnBufferFactory;

public final class DoubleColumnBufferFactory extends ColumnBufferFactory {

    private static final String DOUBLE_TYPE_ID_1 = "float";
    private static final String DOUBLE_TYPE_ID_2 = "float8";
    private static final String DOUBLE_TYPE_ID_3 = "double precision";

    @Override
    public boolean supportsColumnType(String type, int precision, int scale, boolean nullable) {
        return type.equalsIgnoreCase(DOUBLE_TYPE_ID_1) || type.equalsIgnoreCase(DOUBLE_TYPE_ID_2) || type.equalsIgnoreCase(DOUBLE_TYPE_ID_3);
    }

    @Override
    protected ColumnBuffer<?> createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        return new DoubleColumnBuffer(maxRowCount, name, index, nullable);
    }

    private static final class DoubleColumnBuffer extends ColumnBuffer<Double> {

        private static final int DOUBLE_SIZE = 8;

        public DoubleColumnBuffer(int rows, String name, int index, boolean nullable) {
            super(rows, DOUBLE_SIZE, DOUBLE_SIZE, name, index, nullable);
        }

        @Override
        protected void bufferNextValue(Double source, ByteBuffer buffer) {
            buffer.putDouble(source.doubleValue());
        }

        @Override
        protected void write(VectorSink target, int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException {
            target.writeDoubleColumn(columnIndex, values, markers);
        }
    }
}
