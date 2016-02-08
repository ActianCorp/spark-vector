package com.actian.spark_vectorh.buffer.decimal;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import com.actian.spark_vectorh.buffer.VectorSink;
import com.actian.spark_vectorh.buffer.decimal.DecimalColumnBufferFactory.DecimalColumnBuffer;

public final class DecimalLongColumnBufferFactory extends DecimalColumnBufferFactory {

    private static final int MIN_LONG_PRECISION = 10;
    private static final int MAX_LONG_PRECISION = 18;

    @Override
    protected DecimalColumnBuffer createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        return new DecimalLongColumnBuffer(maxRowCount, name, index, precision, scale, nullable);
    }

    private static final class DecimalLongColumnBuffer extends DecimalColumnBuffer {

        private static final int DECIMAL_LONG_SIZE = 8;

        public DecimalLongColumnBuffer(int rows, String name, int index, int precision, int scale, boolean nullable) {
            super(rows, DECIMAL_LONG_SIZE, name, index, precision, scale, nullable);
        }

        @Override
        protected void bufferNextScaledValue(BigDecimal scaled, ByteBuffer buffer) {
            buffer.putLong(scaled.longValue());
        }

        @Override
        protected void write(VectorSink target, int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException {
            target.writeLongColumn(columnIndex, values, markers);
        }
    }

    @Override
    protected int minPrecision() {
        return MIN_LONG_PRECISION;
    }

    @Override
    protected int maxPrecision() {
        return MAX_LONG_PRECISION;
    }
}
