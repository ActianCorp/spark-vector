package com.actian.spark_vectorh.buffer.decimal;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import com.actian.spark_vectorh.buffer.VectorSink;

public final class DecimalByteColumnBufferFactory extends DecimalColumnBufferFactory {

    private static final int MAX_BYTE_PRECISION = 2;

    @Override
    protected DecimalByteColumnBuffer createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable,
            int maxRowCount) {
        return new DecimalByteColumnBuffer(maxRowCount, name, index, precision, scale, nullable);
    }

    private static final class DecimalByteColumnBuffer extends DecimalColumnBuffer {

        private static final int DECIMAL_BYTE_SIZE = 1;

        public DecimalByteColumnBuffer(int rows, String name, int index, int precision, int scale, boolean nullable) {
            super(rows, DECIMAL_BYTE_SIZE, name, index, precision, scale, nullable);
        }

        @Override
        protected void bufferNextScaledValue(BigDecimal scaled, ByteBuffer buffer) {
            buffer.put(scaled.byteValue());
        }

        @Override
        protected void write(VectorSink target, int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException {
            target.writeByteColumn(columnIndex, values, markers);
        }
    }

    @Override
    protected int minPrecision() {
        return 0;
    }

    @Override
    protected int maxPrecision() {
        return MAX_BYTE_PRECISION;
    }
}
