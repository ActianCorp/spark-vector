package com.actian.spark_vectorh.buffer.decimal;

import static com.actian.spark_vectorh.buffer.commons.BigIntegerConversion.convertToDoubleLongByteArray;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import com.actian.spark_vectorh.buffer.VectorSink;
import com.actian.spark_vectorh.buffer.decimal.DecimalColumnBufferFactory.DecimalColumnBuffer;

public final class DecimalDoubleLongColumnBufferFactory extends DecimalColumnBufferFactory {

    private static final int MIN_DOUBLE_LONG_PRECISION = 19;
    private static final int MAX_DECIMAL_PRECISION = 38;

    @Override
    protected DecimalColumnBuffer createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        return new DecimalDoubleLongColumnBuffer(maxRowCount, name, index, precision, scale, nullable);
    }

    private static final class DecimalDoubleLongColumnBuffer extends DecimalColumnBuffer {

        private static final int DECIMAL_DOUBLE_LONG_SIZE = 16;

        public DecimalDoubleLongColumnBuffer(int rows, String name, int index, int precision, int scale, boolean nullable) {
            super(rows, DECIMAL_DOUBLE_LONG_SIZE, name, index, precision, scale, nullable);
        }

        @Override
        protected void bufferNextScaledValue(BigDecimal scaled, ByteBuffer buffer) {
            buffer.put(convertToDoubleLongByteArray(scaled.toBigInteger()));
        }

        @Override
        protected void write(VectorSink target, int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException {
            target.write128BitColumn(columnIndex, values, markers);
        }
    }

    @Override
    protected int minPrecision() {
        return MIN_DOUBLE_LONG_PRECISION;
    }

    @Override
    protected int maxPrecision() {
        return MAX_DECIMAL_PRECISION;
    }
}
