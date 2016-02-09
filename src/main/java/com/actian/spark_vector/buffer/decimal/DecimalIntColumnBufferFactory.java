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
package com.actian.spark_vector.buffer.decimal;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import com.actian.spark_vector.buffer.VectorSink;
import com.actian.spark_vector.buffer.decimal.DecimalColumnBufferFactory.DecimalColumnBuffer;

public final class DecimalIntColumnBufferFactory extends DecimalColumnBufferFactory {

    private static final int MIN_INT_PRECISION = 5;
    private static final int MAX_INT_PRECISION = 9;

    @Override
    protected DecimalColumnBuffer createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        return new DecimalIntColumnBuffer(maxRowCount, name, index, precision, scale, nullable);
    }

    private static final class DecimalIntColumnBuffer extends DecimalColumnBuffer {

        private static final int DECIMAL_INT_SIZE = 4;

        public DecimalIntColumnBuffer(int rows, String name, int index, int precision, int scale, boolean nullable) {
            super(rows, DECIMAL_INT_SIZE, name, index, precision, scale, nullable);
        }

        @Override
        protected void bufferNextScaledValue(BigDecimal scaled, ByteBuffer buffer) {
            buffer.putInt(scaled.intValue());
        }

        @Override
        protected void write(VectorSink target, int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException {
            target.writeIntColumn(columnIndex, values, markers);
        }
    }

    @Override
    protected int minPrecision() {
        return MIN_INT_PRECISION;
    }

    @Override
    protected int maxPrecision() {
        return MAX_INT_PRECISION;
    }
}
