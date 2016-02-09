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

import static com.actian.spark_vector.buffer.commons.BigIntegerConversion.convertToDoubleLongByteArray;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import com.actian.spark_vector.buffer.VectorSink;
import com.actian.spark_vector.buffer.decimal.DecimalColumnBufferFactory.DecimalColumnBuffer;

public final class DecimalDoubleLongColumnBufferFactory extends DecimalColumnBufferFactory {

    private static final int MIN_DOUBLE_LONG_PRECISION = 19;
    private static final int MAX_DECIMAL_PRECISION = 38;

    @Override
    protected DecimalColumnBuffer createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        return new DecimalDoubleLongColumnBuffer(maxRowCount, name, index, precision, scale, nullable);
    }

    private static final class DecimalDoubleLongColumnBuffer extends DecimalColumnBuffer {

        private static final int DECIMAL_DOUBLE_LONG_SIZE = 16;

        public DecimalDoubleLongColumnBuffer(int rows, String name, int index, int precision, int scale, boolean nullable) {
            super(rows, DECIMAL_DOUBLE_LONG_SIZE, name, index, precision, scale, nullable);
        }

        @Override
        protected void bufferNextScaledValue(BigDecimal scaled, ByteBuffer buffer) {
            buffer.put(convertToDoubleLongByteArray(scaled.toBigInteger()));
        }

        @Override
        protected void write(VectorSink target, int columnIndex, ByteBuffer values, ByteBuffer markers) throws IOException {
            target.write128BitColumn(columnIndex, values, markers);
        }
    }

    @Override
    protected int minPrecision() {
        return MIN_DOUBLE_LONG_PRECISION;
    }

    @Override
    protected int maxPrecision() {
        return MAX_DECIMAL_PRECISION;
    }
}
