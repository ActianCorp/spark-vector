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
package com.actian.spark_vector.buffer.integer;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.actian.spark_vector.buffer.ColumnBufferFactory;
import com.actian.spark_vector.buffer.VectorSink;
import com.actian.spark_vector.buffer.ColumnBuffer;

public final class ByteColumnBufferFactory extends ColumnBufferFactory {

    private static final String BYTE_TYPE_ID_1 = "tinyint";
    private static final String BYTE_TYPE_ID_2 = "integer1";

    @Override
    public boolean supportsColumnType(String type, int precision, int scale, boolean nullable) {
        return type.equalsIgnoreCase(BYTE_TYPE_ID_1) || type.equalsIgnoreCase(BYTE_TYPE_ID_2);
    }

    @Override
    protected ColumnBuffer<?> createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        return new ByteColumnBuffer(maxRowCount, name, index, nullable);
    }

    private static final class ByteColumnBuffer extends ColumnBuffer<Byte> {

        private static final int BYTE_SIZE = 1;

        public ByteColumnBuffer(int rows, String name, int index, boolean nullable) {
            super(rows, BYTE_SIZE, BYTE_SIZE, name, index, nullable);
        }

        @Override
        protected void bufferNextValue(Byte source, ByteBuffer buffer) {
            buffer.put(source);
        }

        @Override
        protected void write(VectorSink target, int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException {
            target.writeByteColumn(columnIndex, values, markers);
        }
    }
}
