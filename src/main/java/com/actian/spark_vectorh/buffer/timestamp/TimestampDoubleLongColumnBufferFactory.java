package com.actian.spark_vectorh.buffer.timestamp;

import static com.actian.spark_vectorh.buffer.commons.BigIntegerConversion.convertToDoubleLongByteArray;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import com.actian.spark_vectorh.buffer.VectorSink;
import com.actian.spark_vectorh.buffer.timestamp.TimestampConversion.TimestampConverter;
import com.actian.spark_vectorh.buffer.timestamp.TimestampColumnBufferFactory.TimestampColumnBuffer;

abstract class TimestampDoubleLongColumnBufferFactory extends TimestampColumnBufferFactory {
    protected boolean adjustToUTC() {
        return true;
    }

    @Override
    protected final TimestampColumnBuffer createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable,
            int maxRowCount) {
        return new TimestampDoubleLongColumnBuffer(maxRowCount, name, index, scale, nullable, createConverter(), adjustToUTC());
    }

    @Override
    protected abstract TimestampConverter createConverter();

    private static final class TimestampDoubleLongColumnBuffer extends TimestampColumnBuffer {

        private static final int TIMESTAMP_DOUBLE_LONG_SIZE = 16;

        private TimestampDoubleLongColumnBuffer(int valueCount, String name, int index, int scale, boolean nullable, TimestampConverter converter,
                boolean adjustToUTC) {
            super(valueCount, TIMESTAMP_DOUBLE_LONG_SIZE, name, index, scale, nullable, converter, adjustToUTC);
        }

        @Override
        protected void bufferNextConvertedValue(BigInteger converted, ByteBuffer buffer) {
            buffer.put(convertToDoubleLongByteArray(converted));
        }

        @Override
        protected void write(VectorSink target, int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException {
            target.write128BitColumn(columnIndex, values, markers);
        }
    }
}
