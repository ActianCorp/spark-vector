package com.actian.spark_vectorh.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface VectorSink {
    public void writeByteColumn(int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException;

    public void writeShortColumn(int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException;

    public void writeIntColumn(int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException;

    public void writeLongColumn(int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException;

    public void write128BitColumn(int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException;

    public void writeFloatColumn(int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException;

    public void writeDoubleColumn(int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException;

    public void writeStringColumn(int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException;
}
