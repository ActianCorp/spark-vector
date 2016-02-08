package com.actian.spark_vectorh.buffer.decimal;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import com.actian.spark_vectorh.buffer.VectorSink;
import com.actian.spark_vectorh.buffer.decimal.DecimalColumnBufferFactory.DecimalColumnBuffer;

public final class DecimalShortColumnBufferFactory extends DecimalColumnBufferFactory {

    private static final int MIN_SHORT_PRECISION = 3;
    private static final int MAX_SHORT_PRECISION = 4;

    @Override
    protected DecimalColumnBuffer createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        return new DecimalShortColumnBuffer(maxRowCount, name, index, precision, scale, nullable);
    }

    private static final class DecimalShortColumnBuffer extends DecimalColumnBuffer {

        private static final int DECIMAL_SHORT_SIZE = 2;

        public DecimalShortColumnBuffer(int rows, String name, int index, int precision, int scale, boolean nullable) {
            super(rows, DECIMAL_SHORT_SIZE, name, index, precision, scale, nullable);
        }

        @Override
        protected void bufferNextScaledValue(BigDecimal scaled, ByteBuffer buffer) {
            buffer.putShort(scaled.shortValue());
        }

        @Override
        protected void write(VectorSink target, int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException {
            target.writeShortColumn(columnIndex, values, markers);
        }
    }

    @Override
    protected int minPrecision() {
        return MIN_SHORT_PRECISION;
    }

    @Override
    protected int maxPrecision() {
        return MAX_SHORT_PRECISION;
    }
}
