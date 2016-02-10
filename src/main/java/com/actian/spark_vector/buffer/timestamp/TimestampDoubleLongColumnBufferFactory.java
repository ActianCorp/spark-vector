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
package com.actian.spark_vector.buffer.timestamp;

import static com.actian.spark_vector.buffer.commons.BigIntegerConversion.convertToDoubleLongByteArray;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import com.actian.spark_vector.buffer.VectorSink;
import com.actian.spark_vector.buffer.timestamp.TimestampConversion.TimestampConverter;
import com.actian.spark_vector.buffer.timestamp.TimestampColumnBufferFactory.TimestampColumnBuffer;

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
