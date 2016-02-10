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

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import com.actian.spark_vector.buffer.ColumnBuffer;
import com.actian.spark_vector.buffer.ColumnBufferFactory;

abstract class DecimalColumnBufferFactory extends ColumnBufferFactory {

    private static final String DECIMAL_TYPE_ID = "decimal";

    @Override
    public final boolean supportsColumnType(String type, int precision, int scale, boolean nullable) {
        return type.equalsIgnoreCase(DECIMAL_TYPE_ID) && 0 < precision && 0 <= scale && scale <= precision && minPrecision() <= precision
                && precision <= maxPrecision();
    }

    @Override
    protected abstract DecimalColumnBuffer createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable,
            int maxRowCount);

    protected abstract int minPrecision();

    protected abstract int maxPrecision();

    protected static abstract class DecimalColumnBuffer extends ColumnBuffer<Number> {

        private final int scale;
        private final int precision;

        public DecimalColumnBuffer(int rows, int valueWidth, String name, int index, int precision, int scale, boolean nullable) {
            super(rows, valueWidth, valueWidth, name, index, nullable);
            this.scale = scale;
            this.precision = precision;
        }

        @Override
        protected final void bufferNextValue(Number source, ByteBuffer buffer) {
            bufferNextScaledValue(movePoint(new BigDecimal(source.toString()), precision, scale), buffer);
        }

        protected abstract void bufferNextScaledValue(BigDecimal scaled, ByteBuffer buffer);
    }

    private static BigDecimal movePoint(BigDecimal value, int precision, int scale) {
        int sourceIntegerDigits = value.precision() - value.scale();
        int targetIntegerDigits = precision - scale;
        int moveRightBy = sourceIntegerDigits > targetIntegerDigits ? scale - (sourceIntegerDigits - targetIntegerDigits) : scale;
        return value.movePointRight(moveRightBy);
    }
}
