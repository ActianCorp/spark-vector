package com.actian.spark_vectorh.buffer;

public abstract class ColumnBufferFactory {

    public final ColumnBuffer<?> createColumnBuffer(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        if (!supportsColumnType(type, precision, scale, nullable)) {
            throw new IllegalArgumentException("Target column type [name=" + type + ", precision=" + precision + ", scale=" + scale + ", nullable= " + nullable
                    + "] is not supported!");
        }

        return createColumnBufferInternal(name, index, type, precision, scale, nullable, maxRowCount);
    }

    public abstract boolean supportsColumnType(String type, int precision, int scale, boolean nullable);

    protected abstract ColumnBuffer<?> createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable,
            int maxRowCount);
}
