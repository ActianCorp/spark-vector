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
import com.actian.spark_vector.buffer.string.ByteEncodedStringColumnBufferFactoryCommons.ByteEncodedStringColumnBuffer;

abstract class CharLengthLimitedStringColumnBufferFactory extends StringColumnBufferFactory {

    @Override
    protected final ColumnBuffer<?> createColumnBufferInternal(String name, int index, String type, int precision, int scale, boolean nullable, int maxRowCount) {
        return new CharLimitedStringColumnBuffer(maxRowCount, name, index, precision, nullable);
    }

    private static final class CharLimitedStringColumnBuffer extends ByteEncodedStringColumnBuffer {

        private static final int MAX_UTF_8_CHAR_SIZE = 4;

        private final int precision;

        public CharLimitedStringColumnBuffer(int rows, String name, int index, int precision, boolean nullable) {
            super(rows, name, index, precision * MAX_UTF_8_CHAR_SIZE, nullable);
            this.precision = precision;
        }

        @Override
        protected byte[] encodeNextValue(String value) {
            return StringConversion.truncateToUTF16CodeUnits(value, precision);
        }
    }
}
