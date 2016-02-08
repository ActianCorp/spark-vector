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
package com.actian.spark_vectorh.buffer.string;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.actian.spark_vectorh.buffer.VectorSink;
import com.actian.spark_vectorh.buffer.ColumnBuffer;

final class IntegerEncodedStringColumnBufferFactoryCommons {

    private IntegerEncodedStringColumnBufferFactoryCommons() {
        throw new IllegalStateException();
    }

    public static abstract class IntegerEncodedStringColumnBuffer extends ColumnBuffer<String> {

        private static final int CHAR_SIZE = 4;
        protected static final char WHITESPACE = '\u0020';

        protected IntegerEncodedStringColumnBuffer(int rows, String name, int index, boolean nullable) {
            super(rows, CHAR_SIZE, CHAR_SIZE, name, index, nullable);
        }

        @Override
        protected void bufferNextValue(String source, ByteBuffer buffer) {
            String value = source;
            if (value.isEmpty()) {
                buffer.putInt(WHITESPACE);
            } else {
                buffer.putInt(encodeNextValue(value));
            }
        }

        @Override
        protected void write(VectorSink target, int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException {
            target.writeIntColumn(columnIndex, values, markers);
        }

        protected abstract int encodeNextValue(String value);
    }
}
