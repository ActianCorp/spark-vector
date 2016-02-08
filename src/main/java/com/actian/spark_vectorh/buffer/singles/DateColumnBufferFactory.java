package com.actian.spark_vectorh.buffer.singles;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.actian.spark_vectorh.buffer.VectorSink;
import com.actian.spark_vectorh.buffer.ColumnBuffer;
import com.actian.spark_vectorh.buffer.ColumnBufferFactory;
import com.actian.spark_vectorh.buffer.time.TimeConversion;

import java.sql.Date;

public final class DateColumnBufferFactory extends ColumnBufferFactory {

    private static final String ANSIDATE_TYPE_ID = "ansidate";
    private static final int DAYS_BEFORE_EPOCH = 719528;

    @Override
    public boolean supportsColumnType(String type, int precision, int scale, boolean nullable) {
        return type.equalsIgnoreCase(ANSIDATE_TYPE_ID);
    }

    @Override
    protected ColumnBuffer<?> createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        return new DateColumnBuffer(maxRowCount, name, index, nullable);
    }

    private static final class DateColumnBuffer extends ColumnBuffer<Date> {

        private static final int DATE_SIZE = 4;

        protected DateColumnBuffer(int rows, String name, int index, boolean nullable) {
            super(rows, DATE_SIZE, DATE_SIZE, name, index, nullable);
        }

        @Override
        protected void bufferNextValue(Date source, ByteBuffer buffer) {
            TimeConversion.convertLocalDateToUTC(source);
            buffer.putInt((int) (source.getTime() / TimeConversion.MILLISECONDS_IN_DAY + DAYS_BEFORE_EPOCH));
        }

        @Override
        protected void write(VectorSink target, int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException {
            target.writeIntColumn(columnIndex, values, markers);
        }
    }
}
