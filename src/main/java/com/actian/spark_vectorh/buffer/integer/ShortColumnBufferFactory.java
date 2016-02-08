package com.actian.spark_vectorh.buffer.integer;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.actian.spark_vectorh.buffer.ColumnBufferFactory;
import com.actian.spark_vectorh.buffer.VectorSink;
import com.actian.spark_vectorh.buffer.ColumnBuffer;

public final class ShortColumnBufferFactory extends ColumnBufferFactory {

    private static final String SHORT_TYPE_ID_1 = "smallint";
    private static final String SHORT_TYPE_ID_2 = "integer2";

    @Override
    public boolean supportsColumnType(String type, int precision, int scale, boolean nullable) {
        return type.equalsIgnoreCase(SHORT_TYPE_ID_1) || type.equalsIgnoreCase(SHORT_TYPE_ID_2);
    }

    @Override
    protected ColumnBuffer<?> createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        return new ShortColumnBuffer(maxRowCount, name, index, nullable);
    }

    private static final class ShortColumnBuffer extends ColumnBuffer<Short> {

        private static final int SHORT_SIZE = 2;

        public ShortColumnBuffer(int rows, String name, int index, boolean nullable) {
            super(rows, SHORT_SIZE, SHORT_SIZE, name, index, nullable);
        }

        @Override
        protected void bufferNextValue(Short source, ByteBuffer buffer) {
            buffer.putShort(source);
        }

        @Override
        protected void write(VectorSink target, int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException {
            target.writeShortColumn(columnIndex, values, markers);
        }
    }
}
