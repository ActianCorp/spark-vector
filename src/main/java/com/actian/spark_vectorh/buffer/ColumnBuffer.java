/*
 * Copyright 2016 Actian Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.actian.spark_vectorh.buffer;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public abstract class ColumnBuffer<T> {

    private static final byte NULL_MARKER = (byte) 1;
    private static final byte NON_NULL_MARKER = (byte) 0;

    private final String name;
    private final int index;
    private final boolean nullable;
    private final ByteBuffer values;
    private final ByteBuffer markers;
    private final int valueWidth;
    private final byte[] nullValue;
    private final int alignReq;

    public Class<?> getParameterType() {
        Class<?> ret = this.getClass();
        while (ret.getSuperclass() != ColumnBuffer.class) {
            ret = ret.getSuperclass();
        }
        return (Class<?>) (((ParameterizedType) ret.getGenericSuperclass()).getActualTypeArguments()[0]);
    }

    protected ColumnBuffer(int valueCount, int valueWidth, int alignReq, String name, int index, boolean nullable) {
        this.name = name;
        this.index = index;
        this.nullable = nullable;
        this.values = ByteBuffer.allocateDirect(valueCount * valueWidth).order(ByteOrder.nativeOrder());
        this.markers = nullable ? ByteBuffer.allocateDirect(valueCount).order(ByteOrder.nativeOrder()) : null;
        this.valueWidth = valueWidth;
        this.nullValue = new byte[alignReq];
        Arrays.fill(this.nullValue, (byte) 0);
        this.alignReq = alignReq;
    }

    protected abstract void bufferNextValue(T source, ByteBuffer buffer);

    public final void bufferNextValue(T source) {
        bufferNextValue(source, values);
        if (nullable) {
            markers.put(NON_NULL_MARKER);
        }
    }

    public final void bufferNextNullValue() {
        if (nullable) {
            markers.put(NULL_MARKER);
            values.put(nullValue);
        } else {
            throw new IllegalArgumentException("Can not store NULL values in a non-nullable column '" + name + "'.");
        }
    }

    public final int getBufferSize() {
        int ret = values.position();
        if (nullable) {
            ret += markers.position();
        }
        return ret;
    }

    public final int getValueWidth() {
        return valueWidth;
    }

    public final int getAlignReq() {
        return alignReq;
    }

    public final void writeBufferedValues(VectorSink target) throws IOException {
        values.flip();
        if (nullable) {
            markers.flip();
        }
        write(target, index, values, markers);
        values.clear();
        if (nullable) {
            markers.clear();
        }
    }

    protected abstract void write(VectorSink target, int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException;
}
