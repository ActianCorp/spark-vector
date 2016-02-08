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
package com.actian.spark_vectorh.buffer.timestamp;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import com.actian.spark_vectorh.buffer.VectorSink;
import com.actian.spark_vectorh.buffer.timestamp.TimestampConversion.TimestampConverter;
import com.actian.spark_vectorh.buffer.timestamp.TimestampColumnBufferFactory.TimestampColumnBuffer;

abstract class TimestampLongColumnBufferFactory extends TimestampColumnBufferFactory {
    protected boolean adjustToUTC() {
        return true;
    }

    @Override
    protected final TimestampColumnBuffer createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable,
            int maxRowCount) {
        return new TimestampLongColumnBuffer(maxRowCount, name, index, scale, nullable, createConverter(), adjustToUTC());
    }

    @Override
    protected abstract TimestampConverter createConverter();

    private static final class TimestampLongColumnBuffer extends TimestampColumnBuffer {

        private static final int LONG_TIMESTAMP_SIZE = 8;

        private TimestampLongColumnBuffer(int valueCount, String name, int index, int scale, boolean nullable, TimestampConverter converter, boolean adjustToUTC) {
            super(valueCount, LONG_TIMESTAMP_SIZE, name, index, scale, nullable, converter, adjustToUTC);
        }

        @Override
        protected void bufferNextConvertedValue(BigInteger converted, ByteBuffer buffer) {
            buffer.putLong(converted.longValue());
        }

        @Override
        protected void write(VectorSink target, int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException {
            target.writeLongColumn(columnIndex, values, markers);
        }
    }
}
