package com.actian.spark_vectorh.buffer.real;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.actian.spark_vectorh.buffer.VectorSink;
import com.actian.spark_vectorh.buffer.ColumnBuffer;
import com.actian.spark_vectorh.buffer.ColumnBufferFactory;

public final class FloatColumnBufferFactory extends ColumnBufferFactory {

    private static final String FLOAT_TYPE_ID_1 = "float4";
    private static final String FLOAT_TYPE_ID_2 = "real";

    @Override
    public boolean supportsColumnType(String type, int precision, int scale, boolean nullable) {
        return type.equalsIgnoreCase(FLOAT_TYPE_ID_1) || type.equalsIgnoreCase(FLOAT_TYPE_ID_2);
    }

    @Override
    protected ColumnBuffer<?> createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        return new FloatColumnBuffer(maxRowCount, name, index, nullable);
    }

    private static final class FloatColumnBuffer extends ColumnBuffer<Float> {

        private static final int FLOAT_SIZE = 4;

        public FloatColumnBuffer(int rows, String name, int index, boolean nullable) {
            super(rows, FLOAT_SIZE, FLOAT_SIZE, name, index, nullable);
        }

        @Override
        public void bufferNextValue(Float source, ByteBuffer buffer) {
            buffer.putFloat(source.floatValue());
        }

        @Override
        public void write(VectorSink target, int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException {
            target.writeFloatColumn(columnIndex, values, markers);
        }
    }
}
