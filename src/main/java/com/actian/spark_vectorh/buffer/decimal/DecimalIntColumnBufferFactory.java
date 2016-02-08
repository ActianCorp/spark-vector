package com.actian.spark_vectorh.buffer.decimal;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import com.actian.spark_vectorh.buffer.VectorSink;
import com.actian.spark_vectorh.buffer.decimal.DecimalColumnBufferFactory.DecimalColumnBuffer;

public final class DecimalIntColumnBufferFactory extends DecimalColumnBufferFactory {

    private static final int MIN_INT_PRECISION = 5;
    private static final int MAX_INT_PRECISION = 9;

    @Override
    protected DecimalColumnBuffer createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        return new DecimalIntColumnBuffer(maxRowCount, name, index, precision, scale, nullable);
    }

    private static final class DecimalIntColumnBuffer extends DecimalColumnBuffer {

        private static final int DECIMAL_INT_SIZE = 4;

        public DecimalIntColumnBuffer(int rows, String name, int index, int precision, int scale, boolean nullable) {
            super(rows, DECIMAL_INT_SIZE, name, index, precision, scale, nullable);
        }

        @Override
        protected void bufferNextScaledValue(BigDecimal scaled, ByteBuffer buffer) {
            buffer.putInt(scaled.intValue());
        }

        @Override
        protected void write(VectorSink target, int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException {
            target.writeIntColumn(columnIndex, values, markers);
        }
    }

    @Override
    protected int minPrecision() {
        return MIN_INT_PRECISION;
    }

    @Override
    protected int maxPrecision() {
        return MAX_INT_PRECISION;
    }
}
