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
package com.actian.spark_vectorh.buffer.time;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.actian.spark_vectorh.buffer.VectorSink;
import com.actian.spark_vectorh.buffer.time.TimeConversion.TimeConverter;
import com.actian.spark_vectorh.buffer.time.TimeColumnBufferFactory.TimeColumnBuffer;

abstract class TimeIntColumnBufferFactory extends TimeColumnBufferFactory {
    protected boolean adjustToUTC() {
        return true;
    }

    @Override
    protected final TimeColumnBuffer createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        return new TimeIntColumnBuffer(maxRowCount, name, index, scale, nullable, createConverter(), adjustToUTC());
    }

    private static final class TimeIntColumnBuffer extends TimeColumnBuffer {

        private static final int TIME_INT_SIZE = 4;

        protected TimeIntColumnBuffer(int rows, String name, int index, int scale, boolean nullable, TimeConverter converter, boolean adjustToUTC) {
            super(rows, TIME_INT_SIZE, name, index, scale, nullable, converter, adjustToUTC);
        }

        @Override
        protected void bufferNextConvertedValue(long converted, ByteBuffer buffer) {
            buffer.putInt((int) converted);
        }

        @Override
        protected void write(VectorSink target, int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException {
            target.writeIntColumn(columnIndex, values, markers);
        }
    }
}
