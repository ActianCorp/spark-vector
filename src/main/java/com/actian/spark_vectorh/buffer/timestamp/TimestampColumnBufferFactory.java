package com.actian.spark_vectorh.buffer.timestamp;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import com.actian.spark_vectorh.buffer.ColumnBuffer;
import com.actian.spark_vectorh.buffer.ColumnBufferFactory;
import com.actian.spark_vectorh.buffer.timestamp.TimestampConversion.TimestampConverter;
import com.actian.spark_vectorh.buffer.time.TimeConversion;
import java.sql.Timestamp;

abstract class TimestampColumnBufferFactory extends ColumnBufferFactory {
    protected abstract TimestampConverter createConverter();

    public static abstract class TimestampColumnBuffer extends ColumnBuffer<Timestamp> {

        private final int scale;
        private final TimestampConverter converter;
        protected boolean adjustToUTC;

        protected TimestampColumnBuffer(int valueCount, int valueWidth, String name, int index, int scale, boolean nullable, TimestampConverter converter,
                boolean adjustToUTC) {
            super(valueCount, valueWidth, valueWidth, name, index, nullable);
            this.scale = scale;
            this.converter = converter;
            this.adjustToUTC = adjustToUTC;
        }

        @Override
        protected final void bufferNextValue(Timestamp source, ByteBuffer buffer) {
            System.out.println("Trying to buffer timestamp " + source);
            if (adjustToUTC)
                TimeConversion.convertLocalTimeStampToUTC(source);
            System.out.println("After adjusting to local time => " + source);
            System.out.println("Final value is " + converter.convert(source.getTime() / 1000, source.getNanos(), 0, scale));
            bufferNextConvertedValue(converter.convert(source.getTime() / 1000, source.getNanos(), 0, scale), buffer);
        }

        protected abstract void bufferNextConvertedValue(BigInteger converted, ByteBuffer buffer);
    }

    @Override
    protected abstract TimestampColumnBuffer createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable,
            int maxRowCount);
}
