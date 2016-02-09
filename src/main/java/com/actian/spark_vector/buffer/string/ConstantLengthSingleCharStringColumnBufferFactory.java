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
package com.actian.spark_vector.buffer.string;

import com.actian.spark_vector.buffer.ColumnBuffer;
import com.actian.spark_vector.buffer.string.IntegerEncodedStringColumnBufferFactoryCommons.IntegerEncodedStringColumnBuffer;

public final class ConstantLengthSingleCharStringColumnBufferFactory extends StringColumnBufferFactory {

    private static final String NCHAR_TYPE_ID = "nchar";

    @Override
    public boolean supportsColumnType(String type, int precision, int scale, boolean nullable) {
        return type.equalsIgnoreCase(NCHAR_TYPE_ID) && precision == 1;
    }

    @Override
    protected ColumnBuffer<?> createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        return new ConstantLengthSingleCharStringColumnBuffer(maxRowCount, name, index, nullable);
    }

    private static final class ConstantLengthSingleCharStringColumnBuffer extends IntegerEncodedStringColumnBuffer {

        public ConstantLengthSingleCharStringColumnBuffer(int rows, String name, int index, boolean nullable) {
            super(rows, name, index, nullable);
        }

        @Override
        protected int encodeNextValue(String value) {
            if (Character.isHighSurrogate(value.charAt(0))) {
                return WHITESPACE;
            } else {
                return value.codePointAt(0);
            }
        }
    }
}
