package com.actian.spark_vectorh.buffer.time;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.actian.spark_vectorh.buffer.VectorSink;
import com.actian.spark_vectorh.buffer.time.TimeConversion.TimeConverter;
import com.actian.spark_vectorh.buffer.time.TimeColumnBufferFactory.TimeColumnBuffer;

abstract class TimeLongColumnBufferFactory extends TimeColumnBufferFactory {
    protected boolean adjustToUTC() {
        return true;
    }

    @Override
    protected final TimeColumnBuffer createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        return new TimeLongColumnBuffer(maxRowCount, name, index, scale, nullable, createConverter(), adjustToUTC());
    }

    private static final class TimeLongColumnBuffer extends TimeColumnBuffer {

        private static final int TIME_LONG_SIZE = 8;

        protected TimeLongColumnBuffer(int rows, String name, int index, int scale, boolean nullable, TimeConverter converter, boolean adjustToUTC) {
            super(rows, TIME_LONG_SIZE, name, index, scale, nullable, converter, adjustToUTC);
        }

        @Override
        protected void bufferNextConvertedValue(long converted, ByteBuffer buffer) {
            buffer.putLong(converted);
        }

        @Override
        protected void write(VectorSink target, int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException {
            target.writeLongColumn(columnIndex, values, markers);
        }
    }
}
