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
package com.actian.spark_vector.buffer.real;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.actian.spark_vector.buffer.VectorSink;
import com.actian.spark_vector.buffer.ColumnBuffer;
import com.actian.spark_vector.buffer.ColumnBufferFactory;

public final class DoubleColumnBufferFactory extends ColumnBufferFactory {

    private static final String DOUBLE_TYPE_ID_1 = "float";
    private static final String DOUBLE_TYPE_ID_2 = "float8";
    private static final String DOUBLE_TYPE_ID_3 = "double precision";

    @Override
    public boolean supportsColumnType(String type, int precision, int scale, boolean nullable) {
        return type.equalsIgnoreCase(DOUBLE_TYPE_ID_1) || type.equalsIgnoreCase(DOUBLE_TYPE_ID_2) || type.equalsIgnoreCase(DOUBLE_TYPE_ID_3);
    }

    @Override
    protected ColumnBuffer<?> createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        return new DoubleColumnBuffer(maxRowCount, name, index, nullable);
    }

    private static final class DoubleColumnBuffer extends ColumnBuffer<Double> {

        private static final int DOUBLE_SIZE = 8;

        public DoubleColumnBuffer(int rows, String name, int index, boolean nullable) {
            super(rows, DOUBLE_SIZE, DOUBLE_SIZE, name, index, nullable);
        }

        @Override
        protected void bufferNextValue(Double source, ByteBuffer buffer) {
            buffer.putDouble(source.doubleValue());
        }

        @Override
        protected void write(VectorSink target, int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException {
            target.writeDoubleColumn(columnIndex, values, markers);
        }
    }
}
