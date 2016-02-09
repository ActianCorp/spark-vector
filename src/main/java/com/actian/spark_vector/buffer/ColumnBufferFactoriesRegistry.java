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
package com.actian.spark_vector.buffer;

import java.util.ArrayList;
import java.util.List;

import com.actian.spark_vector.buffer.decimal.DecimalByteColumnBufferFactory;
import com.actian.spark_vector.buffer.decimal.DecimalDoubleLongColumnBufferFactory;
import com.actian.spark_vector.buffer.decimal.DecimalIntColumnBufferFactory;
import com.actian.spark_vector.buffer.decimal.DecimalLongColumnBufferFactory;
import com.actian.spark_vector.buffer.decimal.DecimalShortColumnBufferFactory;
import com.actian.spark_vector.buffer.integer.ByteColumnBufferFactory;
import com.actian.spark_vector.buffer.integer.IntColumnBufferFactory;
import com.actian.spark_vector.buffer.integer.LongColumnBufferFactory;
import com.actian.spark_vector.buffer.integer.ShortColumnBufferFactory;
import com.actian.spark_vector.buffer.real.DoubleColumnBufferFactory;
import com.actian.spark_vector.buffer.real.FloatColumnBufferFactory;
import com.actian.spark_vector.buffer.singles.BooleanColumnBufferFactory;
import com.actian.spark_vector.buffer.singles.DateColumnBufferFactory;
import com.actian.spark_vector.buffer.string.ConstantLengthMultiByteStringColumnBufferFactory;
import com.actian.spark_vector.buffer.string.ConstantLengthMultiCharStringColumnBufferFactory;
import com.actian.spark_vector.buffer.string.ConstantLengthSingleByteStringColumnBufferFactory;
import com.actian.spark_vector.buffer.string.ConstantLengthSingleCharStringColumnBufferFactory;
import com.actian.spark_vector.buffer.string.VariableLengthByteStringColumnBufferFactory;
import com.actian.spark_vector.buffer.string.VariableLengthCharStringColumnBufferFactory;
import com.actian.spark_vector.buffer.time.TimeLZIntColumnBufferFactory;
import com.actian.spark_vector.buffer.time.TimeLZLongColumnBufferFactory;
import com.actian.spark_vector.buffer.time.TimeNZIntColumnBufferFactory;
import com.actian.spark_vector.buffer.time.TimeNZLongColumnBufferFactory;
import com.actian.spark_vector.buffer.time.TimeTZIntColumnBufferFactory;
import com.actian.spark_vector.buffer.time.TimeTZLongColumnBufferFactory;
import com.actian.spark_vector.buffer.timestamp.TimestampLZDoubleLongColumnBufferFactory;
import com.actian.spark_vector.buffer.timestamp.TimestampLZLongColumnBufferFactory;
import com.actian.spark_vector.buffer.timestamp.TimestampNZDoubleLongColumnBufferFactory;
import com.actian.spark_vector.buffer.timestamp.TimestampNZLongColumnBufferFactory;
import com.actian.spark_vector.buffer.timestamp.TimestampTZDoubleLongColumnBufferFactory;
import com.actian.spark_vector.buffer.timestamp.TimestampTZLongColumnBufferFactory;

public final class ColumnBufferFactoriesRegistry {

    private static final List<ColumnBufferFactory> factories = new ArrayList<ColumnBufferFactory>();
    static {
        factories.add(new ByteColumnBufferFactory());
        factories.add(new ShortColumnBufferFactory());
        factories.add(new IntColumnBufferFactory());
        factories.add(new LongColumnBufferFactory());
        factories.add(new VariableLengthCharStringColumnBufferFactory());
        factories.add(new ConstantLengthMultiCharStringColumnBufferFactory());
        factories.add(new ConstantLengthSingleCharStringColumnBufferFactory());
        factories.add(new VariableLengthByteStringColumnBufferFactory());
        factories.add(new ConstantLengthMultiByteStringColumnBufferFactory());
        factories.add(new ConstantLengthSingleByteStringColumnBufferFactory());
        factories.add(new DecimalByteColumnBufferFactory());
        factories.add(new DecimalDoubleLongColumnBufferFactory());
        factories.add(new DecimalIntColumnBufferFactory());
        factories.add(new DecimalLongColumnBufferFactory());
        factories.add(new DecimalShortColumnBufferFactory());
        factories.add(new DateColumnBufferFactory());
        factories.add(new BooleanColumnBufferFactory());
        factories.add(new DoubleColumnBufferFactory());
        factories.add(new FloatColumnBufferFactory());
        factories.add(new TimestampLZDoubleLongColumnBufferFactory());
        factories.add(new TimestampLZLongColumnBufferFactory());
        factories.add(new TimestampNZDoubleLongColumnBufferFactory());
        factories.add(new TimestampNZLongColumnBufferFactory());
        factories.add(new TimestampTZDoubleLongColumnBufferFactory());
        factories.add(new TimestampTZLongColumnBufferFactory());
        factories.add(new TimeLZIntColumnBufferFactory());
        factories.add(new TimeLZLongColumnBufferFactory());
        factories.add(new TimeNZIntColumnBufferFactory());
        factories.add(new TimeNZLongColumnBufferFactory());
        factories.add(new TimeTZIntColumnBufferFactory());
        factories.add(new TimeTZLongColumnBufferFactory());
    }

    public ColumnBufferFactory findFactoryForColumn(String type, int precision, int scale, boolean nullable) {
        for (ColumnBufferFactory factory : factories) {
            if (factory.supportsColumnType(type, precision, scale, nullable)) {
                return factory;
            }
        }
        return null;
    }
}
